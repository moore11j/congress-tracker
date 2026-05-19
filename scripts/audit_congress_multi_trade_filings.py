from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
BACKEND = ROOT / "backend"
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

from sqlalchemy import func, select  # noqa: E402

from app.db import SessionLocal  # noqa: E402
from app.models import Event, Filing, Member, Security, Transaction  # noqa: E402


CONGRESS_SOURCES = {"house_fmp", "senate_fmp"}


def _parse_payload(payload_json: str | None) -> dict:
    try:
        payload = json.loads(payload_json or "{}")
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _payload_transaction_id(payload: dict) -> int | None:
    value = payload.get("transaction_id") or payload.get("transactionId")
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.strip().isdigit():
        return int(value.strip())
    external_id = payload.get("external_id")
    if isinstance(external_id, str) and external_id.startswith("congress_tx:"):
        suffix = external_id.split(":", 1)[1]
        if suffix.isdigit():
            return int(suffix)
    return None


def run(limit: int | None = None) -> dict:
    db = SessionLocal()
    try:
        tx_rows = db.execute(
            select(Transaction, Filing, Member, Security)
            .join(Filing, Filing.id == Transaction.filing_id)
            .join(Member, Member.id == Transaction.member_id)
            .outerjoin(Security, Security.id == Transaction.security_id)
            .where(Filing.source.in_(CONGRESS_SOURCES))
            .order_by(Filing.id.desc(), Transaction.id)
        ).all()

        filing_map: dict[int, dict] = {}
        tx_to_filing: dict[int, int] = {}
        tx_symbols: dict[int, str | None] = {}
        for tx, filing, member, security in tx_rows:
            tx_to_filing[tx.id] = filing.id
            tx_symbols[tx.id] = security.symbol if security else None
            entry = filing_map.setdefault(
                filing.id,
                {
                    "filing_id": filing.id,
                    "document_hash": filing.document_hash,
                    "source": filing.source,
                    "member": f"{member.first_name or ''} {member.last_name or ''}".strip(),
                    "member_bioguide_id": member.bioguide_id,
                    "report_date": filing.filing_date.isoformat() if filing.filing_date else None,
                    "document_url": filing.document_url,
                    "expected_row_count": 0,
                    "event_count": 0,
                    "tickers": [],
                    "missing_tickers": [],
                },
            )
            entry["expected_row_count"] += 1
            if security and security.symbol:
                entry["tickers"].append(security.symbol)

        event_tx_ids: set[int] = set()
        payload_filing_counts: defaultdict[int, int] = defaultdict(int)
        for (payload_json,) in db.execute(
            select(Event.payload_json).where(Event.event_type == "congress_trade")
        ):
            payload = _parse_payload(payload_json)
            tx_id = _payload_transaction_id(payload)
            if tx_id is not None:
                event_tx_ids.add(tx_id)
                filing_id = tx_to_filing.get(tx_id)
                if filing_id is not None:
                    filing_map[filing_id]["event_count"] += 1
                continue
            filing_id = payload.get("filing_id")
            if isinstance(filing_id, int):
                payload_filing_counts[filing_id] += 1

        for filing_id, count in payload_filing_counts.items():
            if filing_id in filing_map and filing_map[filing_id]["event_count"] == 0:
                filing_map[filing_id]["event_count"] = count

        for tx_id, filing_id in tx_to_filing.items():
            if tx_id not in event_tx_ids and filing_id in filing_map:
                symbol = tx_symbols.get(tx_id)
                if symbol:
                    filing_map[filing_id]["missing_tickers"].append(symbol)

        affected = [
            {
                **entry,
                "tickers": sorted(set(entry["tickers"])),
                "missing_tickers": sorted(set(entry["missing_tickers"])),
                "suspected_cause": (
                    "parsed transactions missing canonical events"
                    if entry["event_count"] < entry["expected_row_count"]
                    else None
                ),
            }
            for entry in filing_map.values()
            if entry["expected_row_count"] > 1 and entry["event_count"] < entry["expected_row_count"]
        ]
        affected.sort(
            key=lambda item: (
                item["expected_row_count"] - item["event_count"],
                item["expected_row_count"],
            ),
            reverse=True,
        )

        total_filings = len(filing_map)
        multi_row_filings = sum(1 for item in filing_map.values() if item["expected_row_count"] > 1)
        one_event_multi_row = sum(
            1
            for item in filing_map.values()
            if item["expected_row_count"] > 1 and item["event_count"] == 1
        )
        result = {
            "total_filings_inspected": total_filings,
            "filings_with_gt_1_parsed_transaction_row": multi_row_filings,
            "filings_where_event_count_lt_parsed_transaction_count": len(affected),
            "suspicious_one_event_multi_row_filings": one_event_multi_row,
            "top_affected_filings": affected[: limit or 25],
            "notes": [
                "This audits persisted transactions versus canonical events.",
                "If the source ingester skipped transaction rows before persistence, rerun the fixed ingester/backfill against recent source pages.",
            ],
        }
        result["total_missing_events_from_persisted_transactions"] = sum(
            item["expected_row_count"] - item["event_count"] for item in affected
        )
        return result
    finally:
        db.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit congressional multi-transaction filings.")
    parser.add_argument("--limit", type=int, default=25)
    parser.add_argument("--json", action="store_true", help="Emit JSON instead of a compact text report.")
    args = parser.parse_args()
    result = run(limit=args.limit)
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True))
        return

    print(f"total filings inspected: {result['total_filings_inspected']}")
    print(f"filings with >1 parsed transaction row: {result['filings_with_gt_1_parsed_transaction_row']}")
    print(
        "filings where event count < parsed transaction count: "
        f"{result['filings_where_event_count_lt_parsed_transaction_count']}"
    )
    print(f"suspicious one-event multi-row filings: {result['suspicious_one_event_multi_row_filings']}")
    print(f"missing events from persisted transactions: {result['total_missing_events_from_persisted_transactions']}")
    print("")
    print("top affected filings:")
    for item in result["top_affected_filings"]:
        print(
            f"- filing_id={item['filing_id']} member={item['member']} report_date={item['report_date']} "
            f"expected={item['expected_row_count']} actual={item['event_count']} "
            f"missing_tickers={','.join(item['missing_tickers']) or 'unknown'}"
        )


if __name__ == "__main__":
    main()
