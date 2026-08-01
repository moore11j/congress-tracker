from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
from datetime import date, datetime, timezone
from typing import Any

from sqlalchemy import select

from app.db import Base, SessionLocal, engine
from app.models import InsiderTransaction, InsiderTransactionNormalized, SecForm4Filing
from app.services.backtesting.queries import first_text, parse_iso_date, parse_payload
from app.services.sec_form4 import classify_form4_transaction_code
from app.services.ticker_meta import normalize_cik
from app.utils.symbols import normalize_symbol

logger = logging.getLogger(__name__)

PARSER_VERSION = "legacy_fmp_insider_v1"
ACCESSION_RE = re.compile(r"(\d{10}-\d{2}-\d{6})")


def _parse_date_value(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return parse_iso_date(str(value))


def _float_value(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        cleaned = value.strip().replace(",", "").replace("$", "")
        if not cleaned:
            return None
        try:
            return float(cleaned)
        except ValueError:
            return None
    return None


def _bool_text(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y"}


def _accession_from_payload(row: InsiderTransaction, payload: dict[str, Any]) -> str:
    for key in ("accession_number", "accessionNumber", "accessionNo", "accession"):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    url = first_text(payload, "url", "source_url", "sourceUrl", "link", "finalLink")
    match = ACCESSION_RE.search(url or "")
    if match:
        return match.group(1)
    return f"legacy-insider-{row.external_id or row.id}"


def _transaction_code(raw_type: str | None) -> str | None:
    normalized = (raw_type or "").strip().upper()
    if not normalized:
        return None
    if "-" in normalized:
        normalized = normalized.split("-", 1)[0]
    if normalized in {"P", "S", "A", "M", "F", "G"}:
        return normalized
    if "PURCHASE" in normalized or normalized == "BUY":
        return "P"
    if "SALE" in normalized or normalized == "SELL":
        return "S"
    return normalized[:8]


def _relationship(payload: dict[str, Any], role: str | None) -> dict[str, Any]:
    raw = (
        first_text(payload, "typeOfOwner", "type_of_owner", "ownerType", "relationship")
        or role
        or ""
    )
    text = raw.strip()
    lower = text.lower()
    officer_title = None
    if "officer" in lower:
        officer_title = text.split(":", 1)[1].strip() if ":" in text else text
    return {
        "is_director": "director" in lower,
        "is_officer": "officer" in lower or any(term in lower for term in ("ceo", "cfo", "chief", "president")),
        "is_ten_percent_owner": "10 percent" in lower or "ten percent" in lower or "10%" in lower,
        "is_other": bool(text) and not any(term in lower for term in ("director", "officer", "10 percent", "ten percent", "10%")),
        "officer_title": officer_title,
        "raw": text or None,
    }


def _normalized_hash(row: InsiderTransaction, payload: dict[str, Any], accession: str, transaction_code: str | None) -> str:
    parts = [
        "legacy_fmp_insider",
        row.external_id,
        accession,
        payload.get("companyCik") or payload.get("issuer_cik"),
        payload.get("reportingCik") or payload.get("reporting_cik") or row.reporting_cik,
        row.symbol or payload.get("symbol"),
        row.transaction_date or payload.get("transactionDate"),
        transaction_code,
        row.shares or payload.get("securitiesTransacted"),
        row.price or payload.get("price"),
        payload.get("acquisitionOrDisposition"),
        payload.get("directOrIndirect"),
    ]
    value = "|".join("" if item is None else str(item).strip().lower() for item in parts)
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _source_url(payload: dict[str, Any]) -> str | None:
    return first_text(payload, "url", "source_url", "sourceUrl", "link", "finalLink")


def _build_normalized_payload(row: InsiderTransaction) -> tuple[dict[str, Any], dict[str, Any]]:
    payload = parse_payload(row.payload_json)
    accession = _accession_from_payload(row, payload)
    raw_type = row.transaction_type or first_text(payload, "transactionType", "transaction_type", "trade_type")
    transaction_code = _transaction_code(raw_type)
    transaction_type_normalized, transaction_code_description = classify_form4_transaction_code(transaction_code)
    symbol = normalize_symbol(row.symbol or first_text(payload, "symbol", "ticker"))
    issuer_cik = normalize_cik(first_text(payload, "companyCik", "issuer_cik", "issuerCik"))
    reporting_cik = normalize_cik(row.reporting_cik) or normalize_cik(first_text(payload, "reportingCik", "reporting_cik", "rptOwnerCik"))
    reporting_name = row.insider_name or first_text(payload, "reportingName", "reporting_name", "insider_name", "insiderName")
    relationship = _relationship(payload, row.role)
    shares = _float_value(row.shares) or _float_value(first_text(payload, "securitiesTransacted", "shares", "transactionShares"))
    price = _float_value(row.price) or _float_value(first_text(payload, "price", "transactionPrice"))
    value = shares * price if shares is not None and price is not None else None
    transaction_date = row.transaction_date or _parse_date_value(first_text(payload, "transactionDate", "transaction_date"))
    filing_date = row.filing_date or _parse_date_value(first_text(payload, "filingDate", "filing_date"))
    normalized_hash = _normalized_hash(row, payload, accession, transaction_code)
    parser_confidence = 0.82 if reporting_name and relationship.get("raw") else 0.68
    filing_payload = {
        "accession_number": accession,
        "issuer_cik": issuer_cik,
        "issuer_name": first_text(payload, "companyName", "issuer_name", "issuerName"),
        "issuer_trading_symbol": symbol,
        "reporting_owner_cik": reporting_cik,
        "reporting_owner_name": reporting_name,
        "filing_date": filing_date,
        "source_url": _source_url(payload),
        "xml_url": None,
        "raw_metadata_json": json.dumps(payload, sort_keys=True, default=str),
        "parser_status": "parsed",
        "parser_version": PARSER_VERSION,
        "parser_confidence": parser_confidence,
    }
    normalized_payload = {
        "accession_number": accession,
        "issuer_cik": issuer_cik,
        "issuer_name": filing_payload["issuer_name"],
        "ticker_raw": row.symbol or first_text(payload, "symbol", "ticker"),
        "ticker_normalized": symbol,
        "reporting_owner_cik": reporting_cik,
        "reporting_owner_name": reporting_name,
        "owner_relationship_json": json.dumps(relationship, sort_keys=True),
        "officer_title": relationship.get("officer_title"),
        "is_director": bool(relationship.get("is_director")),
        "is_officer": bool(relationship.get("is_officer")),
        "is_ten_percent_owner": bool(relationship.get("is_ten_percent_owner")),
        "transaction_date": transaction_date,
        "filing_date": filing_date,
        "security_title": first_text(payload, "securityName", "security_name", "securityTitle"),
        "transaction_code": transaction_code,
        "transaction_code_description": transaction_code_description,
        "transaction_type_normalized": transaction_type_normalized,
        "shares": shares,
        "price": price,
        "value": value,
        "acquired_disposed": first_text(payload, "acquisitionOrDisposition", "acquired_disposed", "acquiredDisposed"),
        "shares_owned_following": _float_value(first_text(payload, "securitiesOwned", "sharesOwnedFollowingTransaction")),
        "direct_or_indirect": first_text(payload, "directOrIndirect", "direct_or_indirect"),
        "ownership_nature": row.ownership or first_text(payload, "ownershipNature", "ownership_nature"),
        "is_derivative": _bool_text(payload.get("isDerivative")),
        "footnotes_json": json.dumps([], sort_keys=True),
        "ten_b5_1_flag": bool(re.search(r"10b5-?1", json.dumps(payload), flags=re.IGNORECASE)),
        "normalized_hash": normalized_hash,
        "is_duplicate": False,
        "parser_confidence": parser_confidence,
    }
    return filing_payload, normalized_payload


def _get_or_create_filing(db, filing_payload: dict[str, Any], *, apply: bool) -> SecForm4Filing | None:
    accession = filing_payload["accession_number"]
    filing = db.execute(
        select(SecForm4Filing).where(SecForm4Filing.accession_number == accession)
    ).scalar_one_or_none()
    if filing is not None or not apply:
        return filing
    filing = SecForm4Filing(
        accession_number=accession,
        issuer_cik=filing_payload["issuer_cik"],
        issuer_name=filing_payload["issuer_name"],
        issuer_trading_symbol=filing_payload["issuer_trading_symbol"],
        reporting_owner_cik=filing_payload["reporting_owner_cik"],
        reporting_owner_name=filing_payload["reporting_owner_name"],
        filing_date=filing_payload["filing_date"],
        source_url=filing_payload["source_url"],
        xml_url=filing_payload["xml_url"],
        raw_metadata_json=filing_payload["raw_metadata_json"],
        parser_status=filing_payload["parser_status"],
        parser_version=filing_payload["parser_version"],
        parser_confidence=filing_payload["parser_confidence"],
        parsed_at=datetime.now(timezone.utc),
    )
    db.add(filing)
    db.flush()
    return filing


def backfill_legacy_insider_normalized(
    *,
    apply: bool,
    limit: int | None = None,
    batch_size: int = 1000,
    min_id: int | None = None,
    max_id: int | None = None,
) -> dict[str, int | bool | None]:
    Base.metadata.create_all(bind=engine)
    report = {
        "apply": apply,
        "scanned": 0,
        "inserted_filings": 0,
        "inserted_transactions": 0,
        "skipped_existing": 0,
        "skipped_unusable": 0,
        "errors": 0,
        "min_id": min_id,
        "max_id": max_id,
    }
    with SessionLocal() as db:
        query = select(InsiderTransaction).order_by(InsiderTransaction.id.asc())
        if min_id is not None:
            query = query.where(InsiderTransaction.id >= min_id)
        if max_id is not None:
            query = query.where(InsiderTransaction.id <= max_id)
        if limit is not None:
            query = query.limit(limit)
        rows = db.execute(query).scalars().all()
        for row in rows:
            report["scanned"] += 1
            try:
                filing_payload, normalized_payload = _build_normalized_payload(row)
                if not normalized_payload["normalized_hash"] or not normalized_payload["ticker_normalized"]:
                    report["skipped_unusable"] += 1
                    continue
                existing = db.execute(
                    select(InsiderTransactionNormalized.id)
                    .where(InsiderTransactionNormalized.normalized_hash == normalized_payload["normalized_hash"])
                    .limit(1)
                ).scalar_one_or_none()
                if existing is not None:
                    report["skipped_existing"] += 1
                    continue
                filing_before = db.execute(
                    select(SecForm4Filing.id).where(SecForm4Filing.accession_number == filing_payload["accession_number"]).limit(1)
                ).scalar_one_or_none()
                filing = _get_or_create_filing(db, filing_payload, apply=apply)
                if apply and filing_before is None and filing is not None:
                    report["inserted_filings"] += 1
                if apply:
                    db.add(
                        InsiderTransactionNormalized(
                            form4_filing_id=filing.id if filing else None,
                            **normalized_payload,
                        )
                    )
                report["inserted_transactions"] += 1
                if apply and int(report["inserted_transactions"]) % max(batch_size, 1) == 0:
                    db.commit()
            except Exception:
                report["errors"] += 1
                logger.exception("Failed to normalize legacy insider row id=%s", getattr(row, "id", None))
        if apply:
            db.commit()
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill insider_transactions_normalized from legacy insider_transactions.")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--min-id", type=int)
    parser.add_argument("--max-id", type=int)
    parser.add_argument("--batch-size", type=int, default=1000)
    args = parser.parse_args()
    if args.apply == args.dry_run:
        raise SystemExit("Pass exactly one of --dry-run or --apply.")
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    print(
        json.dumps(
            backfill_legacy_insider_normalized(
                apply=args.apply,
                limit=args.limit,
                batch_size=args.batch_size,
                min_id=args.min_id,
                max_id=args.max_id,
            ),
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
