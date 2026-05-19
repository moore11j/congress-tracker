from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any
from urllib.parse import urlparse


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.backfill_events_from_trades import (  # noqa: E402
    _congress_event_from_transaction,
    _congress_event_payload,
    _existing_congress_event_identities,
    insert_missing_congress_events_from_transactions,
)
from app.db import SessionLocal  # noqa: E402
from app.ingest_house import (  # noqa: E402
    _fetch_page as fetch_house_page,
    ingest_house,
    upsert_house_transaction_from_row,
)
from app.ingest_senate import (  # noqa: E402
    _fetch_page as fetch_senate_page,
    ingest_senate,
    upsert_senate_transaction_from_row,
)
from app.models import Event  # noqa: E402
from app.services.congress_metadata import get_congress_metadata_resolver  # noqa: E402
from app.utils.symbols import canonical_symbol  # noqa: E402
from sqlalchemy import select  # noqa: E402


logger = logging.getLogger(__name__)


def _safe_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _row_document_url(row: dict[str, Any]) -> str | None:
    return _safe_str(row.get("link") or row.get("pdf") or row.get("documentUrl") or row.get("document_url"))


def _document_tokens(value: str | None) -> set[str]:
    if not value:
        return set()
    raw = value.strip()
    parsed_path = urlparse(raw).path if "://" in raw else raw
    name = Path(parsed_path.rstrip("/")).name or raw
    tokens = {raw.lower(), name.lower()}
    if name.lower().endswith(".pdf"):
        tokens.add(name[:-4].lower())
    return {token for token in tokens if token}


def _row_matches_document(row: dict[str, Any], document: str) -> bool:
    wanted = _document_tokens(document)
    candidate = _document_tokens(_row_document_url(row))
    for key in ("id", "filingId", "filing_id", "documentId", "document_id"):
        value = _safe_str(row.get(key))
        if value:
            candidate.update(_document_tokens(value))
    return bool(wanted & candidate)


def _row_member_name(row: dict[str, Any]) -> str:
    first = _safe_str(row.get("firstName") or row.get("first_name"))
    last = _safe_str(row.get("lastName") or row.get("last_name"))
    full = _safe_str(row.get("office") or row.get("senator") or row.get("member") or row.get("name"))
    return full or f"{first or ''} {last or ''}".strip()


def _row_symbol(row: dict[str, Any]) -> str | None:
    return canonical_symbol(_safe_str(row.get("symbol") or row.get("ticker")))


def _parse_symbols(value: str | None) -> set[str]:
    if not value:
        return set()
    return {
        symbol
        for symbol in (canonical_symbol(part.strip()) for part in value.split(","))
        if symbol
    }


def _fetch_document_rows(
    *,
    document: str,
    source: str | None,
    pages: int,
    limit: int,
) -> dict[str, Any]:
    sources = [source] if source else ["house", "senate"]
    matches: list[dict[str, Any]] = []
    for source_name in sources:
        fetch_page = fetch_house_page if source_name == "house" else fetch_senate_page
        rows: list[dict[str, Any]] = []
        pages_scanned = 0
        for page in range(pages):
            page_rows = fetch_page(page=page, limit=limit)
            pages_scanned += 1
            if not page_rows:
                break
            rows.extend(row for row in page_rows if _row_matches_document(row, document))
        if rows:
            matches.append({"source": source_name, "rows": rows, "pages_scanned": pages_scanned})

    if not matches:
        raise RuntimeError(
            f"Document {document!r} was not found in the requested source pages; refusing broad fallback."
        )
    if len(matches) > 1:
        found = ", ".join(match["source"] for match in matches)
        raise RuntimeError(f"Document {document!r} matched multiple sources ({found}); pass --source.")
    return matches[0]


def _validate_target_rows(
    rows: list[dict[str, Any]],
    *,
    member: str | None,
    symbols: set[str],
) -> None:
    if member:
        expected = member.strip().lower()
        mismatches = [row for row in rows if expected not in _row_member_name(row).lower()]
        if mismatches:
            names = sorted({_row_member_name(row) for row in mismatches})
            raise RuntimeError(f"Member sanity check failed for {member!r}; parsed members={names}")

    if symbols:
        parsed_symbols = {_row_symbol(row) for row in rows if _row_symbol(row)}
        unexpected = parsed_symbols - symbols
        missing = symbols - parsed_symbols
        if unexpected or missing:
            raise RuntimeError(
                "Symbol sanity check failed: "
                f"expected={sorted(symbols)} parsed={sorted(parsed_symbols)} "
                f"missing={sorted(missing)} unexpected={sorted(unexpected)}"
            )


def _event_exists(
    payload: dict,
    existing_external_ids: set[str],
    existing_transaction_ids: set[int],
    target_backfill_ids: set[str],
) -> bool:
    external_id = str(payload["external_id"])
    backfill_id = str(payload["backfill_id"])
    transaction_id = payload.get("transaction_id")
    return (
        external_id in existing_external_ids
        or transaction_id in existing_transaction_ids
        or backfill_id in target_backfill_ids
    )


def _target_backfill_ids(db, *, filing_id: int | None, document_url: str | None) -> set[str]:
    backfill_ids: set[str] = set()
    for (payload_json,) in db.execute(
        select(Event.payload_json).where(Event.event_type == "congress_trade")
    ):
        try:
            payload = json.loads(payload_json or "{}")
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue
        same_filing = filing_id is not None and payload.get("filing_id") == filing_id
        same_document = document_url and payload.get("document_url") == document_url
        if not (same_filing or same_document):
            continue
        backfill_id = payload.get("backfill_id")
        if isinstance(backfill_id, str) and backfill_id.strip():
            backfill_ids.add(backfill_id.strip())
    return backfill_ids


def _target_row_payload(row: dict[str, Any], outcome: dict[str, Any], *, event_exists: bool, event_inserted: bool) -> dict:
    tx = outcome.get("transaction")
    filing = outcome.get("filing")
    member = outcome.get("member")
    security = outcome.get("security")
    symbol = security.symbol if security is not None else _row_symbol(row)
    action_parts = []
    if outcome["transaction_inserted"]:
        action_parts.append("insert_transaction")
    else:
        action_parts.append("skip_transaction")
    if event_inserted:
        action_parts.append("insert_event")
    else:
        action_parts.append("skip_event")
    return {
        "symbol": symbol,
        "side": tx.transaction_type if tx is not None else _safe_str(row.get("type") or row.get("transactionType")),
        "trade_date": tx.trade_date.isoformat() if tx is not None and tx.trade_date else None,
        "report_date": tx.report_date.isoformat() if tx is not None and tx.report_date else None,
        "amount_range": {
            "min": tx.amount_range_min if tx is not None else None,
            "max": tx.amount_range_max if tx is not None else None,
        },
        "member": f"{member.first_name or ''} {member.last_name or ''}".strip() if member is not None else _row_member_name(row),
        "filing_id": filing.id if filing is not None else None,
        "document_url": filing.document_url if filing is not None else _row_document_url(row),
        "transaction_id": tx.id if tx is not None else None,
        "existing_transaction": not outcome["transaction_inserted"],
        "existing_event": event_exists,
        "action": "+".join(action_parts),
    }


def run_document_repair(
    *,
    document: str,
    apply: bool,
    member: str | None = None,
    symbols: set[str] | None = None,
    source: str | None = None,
    pages: int = 10,
    limit: int = 200,
) -> dict[str, Any]:
    found = _fetch_document_rows(document=document, source=source, pages=pages, limit=limit)
    rows = found["rows"]
    symbols = symbols or set()
    _validate_target_rows(rows, member=member, symbols=symbols)

    mode = "apply" if apply else "dry-run"
    db = SessionLocal()
    try:
        metadata = get_congress_metadata_resolver()
        seen_transaction_keys: set[tuple] = set()
        existing_external_ids, existing_transaction_ids, _existing_backfill_ids = _existing_congress_event_identities(db)
        target_backfill_cache: dict[tuple[int | None, str | None], set[str]] = {}

        per_row = []
        transactions_to_insert = 0
        events_to_insert = 0
        existing_skipped = 0
        duplicates_prevented = 0
        event_rows_inserted = 0

        upsert = upsert_house_transaction_from_row if found["source"] == "house" else upsert_senate_transaction_from_row
        for row in rows:
            outcome = upsert(db, row, metadata=metadata, seen_transaction_keys=seen_transaction_keys)
            tx = outcome.get("transaction")
            if tx is None:
                existing_skipped += 1
                duplicates_prevented += 1
                per_row.append(_target_row_payload(row, outcome, event_exists=False, event_inserted=False))
                continue

            if outcome["transaction_inserted"]:
                transactions_to_insert += 1
            else:
                existing_skipped += 1
                duplicates_prevented += 1

            payload = _congress_event_payload(tx, outcome["filing"], outcome["member"], outcome["security"])
            target_key = (outcome["filing"].id, outcome["filing"].document_url)
            if target_key not in target_backfill_cache:
                target_backfill_cache[target_key] = _target_backfill_ids(
                    db,
                    filing_id=outcome["filing"].id,
                    document_url=outcome["filing"].document_url,
                )
            target_backfill_ids = target_backfill_cache[target_key]
            has_event = _event_exists(payload, existing_external_ids, existing_transaction_ids, target_backfill_ids)
            event_inserted = False
            if not has_event:
                events_to_insert += 1
                event_inserted = True
                if apply:
                    db.add(_congress_event_from_transaction(tx, outcome["filing"], outcome["member"], outcome["security"]))
                    existing_external_ids.add(str(payload["external_id"]))
                    existing_transaction_ids.add(int(payload["transaction_id"]))
                    target_backfill_ids.add(str(payload["backfill_id"]))
                    event_rows_inserted += 1
            else:
                duplicates_prevented += 1

            per_row.append(_target_row_payload(row, outcome, event_exists=has_event, event_inserted=event_inserted))

        if apply:
            db.commit()
        else:
            db.rollback()

        first = per_row[0] if per_row else {}
        result = {
            "mode": mode,
            "scope": "document",
            "source": found["source"],
            "document": document,
            "document_url": first.get("document_url"),
            "member": member or first.get("member"),
            "report_date": first.get("report_date"),
            "pages_scanned": found["pages_scanned"],
            "parsed_transaction_count": len(rows),
            "rows": per_row,
            "totals": {
                "transactions_to_insert": transactions_to_insert,
                "events_to_insert": events_to_insert,
                "events_inserted": event_rows_inserted,
                "existing_skipped": existing_skipped,
                "duplicates_prevented": duplicates_prevented,
            },
        }
        return result
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def _sample_missing_events(limit: int = 10) -> list[dict]:
    db = SessionLocal()
    try:
        existing = insert_missing_congress_events_from_transactions(db, dry_run=True, limit=limit)
        db.rollback()
        return [{"kind": "persisted_transaction_missing_event", "would_insert": existing}]
    finally:
        db.close()


def run_broad(
    *,
    apply: bool,
    pages: int,
    limit: int,
    sleep_s: float,
    skip_source_refresh: bool,
    allow_apply: bool,
) -> dict:
    if apply and not allow_apply:
        raise RuntimeError(
            "Broad apply refused. Pass --all --i-understand-this-is-broad to run an unrestricted production backfill."
        )

    mode = "apply" if apply else "dry-run"
    result: dict[str, object] = {
        "mode": mode,
        "scope": "all",
        "source_refresh": "skipped" if skip_source_refresh else "run",
        "house": None,
        "senate": None,
        "events_inserted": 0,
    }

    if not skip_source_refresh:
        result["house"] = ingest_house(pages=pages, limit=limit, sleep_s=sleep_s, dry_run=not apply)
        result["senate"] = ingest_senate(pages=pages, limit=limit, sleep_s=sleep_s, dry_run=not apply)

    db = SessionLocal()
    try:
        before = db.query(Event).filter(Event.event_type == "congress_trade").count()
        inserted = insert_missing_congress_events_from_transactions(db, dry_run=not apply)
        if apply:
            db.commit()
        else:
            db.rollback()
        after = db.query(Event).filter(Event.event_type == "congress_trade").count()
        result["events_inserted"] = inserted
        result["events_before"] = before
        result["events_after"] = after
    finally:
        db.close()

    if not apply:
        result["sample"] = _sample_missing_events()
        result["note"] = (
            "Dry-run source refresh estimates transaction rows that would be recovered from recent source pages. "
            "Event insertion counts only persisted transactions because dry-run does not write recovered transactions."
        )
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill missing congressional multi-trade events.")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="Preview without writing. This is the default.")
    mode.add_argument("--apply", action="store_true", help="Write changes.")
    parser.add_argument("--document", "--source-document", dest="document", help="Repair exactly one source document.")
    parser.add_argument("--member", help="Optional targeted member sanity filter.")
    parser.add_argument("--symbols", help="Optional comma-separated symbol sanity filter.")
    parser.add_argument("--source", choices=["house", "senate"], help="Optional source hint for targeted document lookup.")
    parser.add_argument("--pages", type=int, default=10, help="Recent source pages to scan.")
    parser.add_argument("--limit", type=int, default=200, help="Rows per source page.")
    parser.add_argument("--sleep-s", type=float, default=0.25)
    parser.add_argument("--skip-source-refresh", action="store_true")
    parser.add_argument("--all", action="store_true", help="Run broad all-document mode.")
    parser.add_argument("--i-understand-this-is-broad", action="store_true", help="Required with --apply --all.")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO))

    apply = bool(args.apply)
    if apply and not args.document and not args.all:
        parser.error("--apply requires --document, or --all --i-understand-this-is-broad for broad mode.")

    if args.document:
        result = run_document_repair(
            document=args.document,
            apply=apply,
            member=args.member,
            symbols=_parse_symbols(args.symbols),
            source=args.source,
            pages=args.pages,
            limit=args.limit,
        )
    else:
        result = run_broad(
            apply=apply,
            pages=args.pages,
            limit=args.limit,
            sleep_s=args.sleep_s,
            skip_source_refresh=args.skip_source_refresh,
            allow_apply=bool(args.all and args.i_understand_this_is_broad),
        )
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
