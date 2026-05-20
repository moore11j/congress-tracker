from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sqlalchemy import select  # noqa: E402

from app.backfill_events_from_trades import _congress_event_from_transaction, _congress_event_payload  # noqa: E402
from app.db import SessionLocal  # noqa: E402
from app.models import Event, Filing, Member, Security, Transaction  # noqa: E402
from app.services.congress_assets import (  # noqa: E402
    CONGRESS_CRYPTO_EVENT_TYPE,
    CONGRESS_TREASURY_EVENT_TYPE,
    classify_congress_disclosure_asset,
)


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("Expected YYYY-MM-DD") from exc


def _payload(value: str | None) -> dict[str, Any]:
    try:
        parsed = json.loads(value or "{}")
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _existing_event_keys(db) -> tuple[set[int], set[str], set[str]]:
    transaction_ids: set[int] = set()
    external_ids: set[str] = set()
    backfill_ids: set[str] = set()
    rows = db.execute(select(Event.payload_json).where(Event.event_type.in_((CONGRESS_TREASURY_EVENT_TYPE, CONGRESS_CRYPTO_EVENT_TYPE)))).all()
    for (payload_json,) in rows:
        payload = _payload(payload_json)
        tx_id = payload.get("transaction_id") or payload.get("transactionId")
        if isinstance(tx_id, int):
            transaction_ids.add(tx_id)
        elif isinstance(tx_id, str) and tx_id.isdigit():
            transaction_ids.add(int(tx_id))
        external_id = payload.get("external_id")
        if isinstance(external_id, str) and external_id.strip():
            external_ids.add(external_id.strip())
        backfill_id = payload.get("backfill_id")
        if isinstance(backfill_id, str) and backfill_id.strip():
            backfill_ids.add(backfill_id.strip())
    return transaction_ids, external_ids, backfill_ids


def _document_id(filing: Filing) -> str | None:
    for value in (filing.document_hash, filing.document_url):
        if not value:
            continue
        text = str(value).rstrip("/")
        return text.rsplit("/", 1)[-1]
    return None


def _candidate_row(tx: Transaction, filing: Filing, member: Member, security: Security | None, existing: tuple[set[int], set[str], set[str]]) -> dict[str, Any] | None:
    raw_symbol = security.symbol if security is not None else None
    raw_asset_class = security.asset_class if security is not None else None
    classification = classify_congress_disclosure_asset(
        security_description=tx.description,
        asset_class=raw_asset_class,
        raw_symbol=raw_symbol,
    )
    if classification is None or classification.event_type not in {CONGRESS_TREASURY_EVENT_TYPE, CONGRESS_CRYPTO_EVENT_TYPE}:
        return None

    payload = _congress_event_payload(tx, filing, member, None)
    if payload is None:
        return None

    existing_tx_ids, existing_external_ids, existing_backfill_ids = existing
    external_id = str(payload.get("external_id") or "")
    backfill_id = str(payload.get("backfill_id") or "")
    already_exists = tx.id in existing_tx_ids or external_id in existing_external_ids or backfill_id in existing_backfill_ids
    details = classification.payload_fields()
    return {
        "transaction_id": tx.id,
        "document_id": _document_id(filing),
        "member": f"{member.first_name or ''} {member.last_name or ''}".strip() or member.bioguide_id,
        "raw_security_description": tx.description,
        "classified_asset_bucket": classification.asset_class,
        "event_type": classification.event_type,
        "instrument_type": classification.instrument_type,
        "duration_label": details.get("duration_label"),
        "maturity_date": details.get("maturity_date"),
        "coupon_rate": details.get("coupon_rate"),
        "cusip": details.get("cusip"),
        "action": "skip" if already_exists else "insert",
        "skip_reason": "already_exists" if already_exists else None,
        "_payload": payload,
        "_tx": tx,
        "_filing": filing,
        "_member": member,
    }


def run(*, since_report_date: date, apply: bool) -> dict[str, int]:
    db = SessionLocal()
    inserted = 0
    insertable = 0
    scanned = 0
    try:
        existing = _existing_event_keys(db)
        rows = db.execute(
            select(Transaction, Filing, Member, Security)
            .join(Filing, Filing.id == Transaction.filing_id)
            .join(Member, Member.id == Transaction.member_id)
            .outerjoin(Security, Security.id == Transaction.security_id)
            .where(Transaction.report_date.is_not(None))
            .where(Transaction.report_date >= since_report_date)
            .where(Filing.source.in_(("house_fmp", "senate_fmp", "house", "senate")))
            .order_by(Transaction.report_date.asc(), Transaction.id.asc())
        ).all()
        for tx, filing, member, security in rows:
            scanned += 1
            candidate = _candidate_row(tx, filing, member, security, existing)
            if candidate is None:
                continue
            public_row = {key: value for key, value in candidate.items() if not key.startswith("_")}
            print(json.dumps(public_row, sort_keys=True))
            if candidate["action"] != "insert":
                continue
            insertable += 1
            if apply:
                event = _congress_event_from_transaction(candidate["_tx"], candidate["_filing"], candidate["_member"], None)
                db.add(event)
                db.flush()
                inserted += 1
                existing[0].add(tx.id)
                existing[1].add(str(candidate["_payload"].get("external_id") or ""))
                existing[2].add(str(candidate["_payload"].get("backfill_id") or ""))
        if apply:
            db.commit()
        return {"scanned": scanned, "insertable": insertable, "inserted": inserted}
    finally:
        db.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Reprocess recent direct Treasury/Crypto Congress disclosures.")
    parser.add_argument("--since-report-date", required=True, type=_parse_date)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    if args.dry_run == args.apply:
        parser.error("Pass exactly one of --dry-run or --apply")
    summary = run(since_report_date=args.since_report_date, apply=args.apply)
    print(json.dumps({"summary": summary, "mode": "apply" if args.apply else "dry_run"}, sort_keys=True))


if __name__ == "__main__":
    main()
