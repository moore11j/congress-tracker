from __future__ import annotations

import hashlib
import json
import re
from datetime import date, datetime, timezone
from typing import Any
from xml.etree import ElementTree as ET

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import Event, InsiderTransactionNormalized, SecForm4Filing, SymbolResolutionOverride
from app.utils.symbols import canonical_symbol

FORM4_PARSER_VERSION = "sec_form4_v1"

TRANSACTION_CODE_DESCRIPTIONS = {
    "P": "Open-market purchase",
    "S": "Open-market sale",
    "A": "Grant or award",
    "M": "Option exercise or conversion",
    "F": "Tax withholding or payment",
    "G": "Gift",
}

TRANSACTION_CODE_NORMALIZED_TYPES = {
    "P": "open_market_purchase",
    "S": "open_market_sale",
    "A": "grant_award",
    "M": "option_exercise_conversion",
    "F": "tax_withholding_payment",
    "G": "gift",
}


def _strip_namespace(tag: str) -> str:
    return tag.rsplit("}", 1)[-1] if "}" in tag else tag


def _children(element: ET.Element, name: str) -> list[ET.Element]:
    return [child for child in list(element) if _strip_namespace(child.tag) == name]


def _child(element: ET.Element, name: str) -> ET.Element | None:
    matches = _children(element, name)
    return matches[0] if matches else None


def _text(element: ET.Element | None, path: str) -> str | None:
    current = element
    for part in path.split("/"):
        if current is None:
            return None
        current = _child(current, part)
    if current is None or current.text is None:
        return None
    parsed = current.text.strip()
    return parsed if parsed else None


def _bool_text(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "yes", "y"}


def _float_text(value: str | None) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(str(value).replace(",", ""))
    except Exception:
        return None


def _parse_date(value: Any) -> date | None:
    if not value:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    cleaned = str(value).strip()
    if not cleaned:
        return None
    try:
        return datetime.fromisoformat(cleaned[:10]).date()
    except Exception:
        return None


def _normalize_cik(value: str | None) -> str | None:
    if not value:
        return None
    digits = "".join(ch for ch in str(value) if ch.isdigit())
    return digits.zfill(10) if digits else None


def normalize_form4_symbol(raw_symbol: str | None, issuer_name: str | None = None, db: Session | None = None) -> tuple[str | None, str]:
    raw = (raw_symbol or "").strip()
    if db is not None:
        override = db.execute(
            select(SymbolResolutionOverride)
            .where(SymbolResolutionOverride.domain_key.in_(("insider_trades", "form4_filings")))
            .where(SymbolResolutionOverride.raw_symbol == (raw or None))
            .where(SymbolResolutionOverride.issuer_name == (issuer_name or None))
            .limit(1)
        ).scalar_one_or_none()
        if override:
            return override.normalized_symbol, "admin_override" if override.normalized_symbol else "admin_unresolved"

    if not raw:
        return None, "unresolved"
    upper = raw.upper()
    if upper in {"BRK.B", "BRK/B"}:
        return "BRK-B", "resolved"
    if upper in {"BF.B", "BF/B"}:
        return "BF-B", "resolved"
    if "." in upper:
        base, suffix = upper.split(".", 1)
        if len(suffix) == 1 and suffix.isalpha() and 1 <= len(base) <= 5:
            return f"{base}-{suffix}", "resolved"
    normalized = canonical_symbol(upper)
    return (normalized, "resolved") if normalized else (None, "unresolved")


def classify_form4_transaction_code(code: str | None) -> tuple[str | None, str | None]:
    normalized = (code or "").strip().upper()
    if not normalized:
        return None, None
    return (
        TRANSACTION_CODE_NORMALIZED_TYPES.get(normalized, f"sec_code_{normalized.lower()}"),
        TRANSACTION_CODE_DESCRIPTIONS.get(normalized, "Other SEC Form 4 transaction"),
    )


def insider_transaction_hash(payload: dict[str, Any]) -> str:
    parts = [
        payload.get("accession_number"),
        payload.get("issuer_cik"),
        payload.get("reporting_owner_cik"),
        payload.get("transaction_date"),
        payload.get("security_title"),
        payload.get("transaction_code"),
        payload.get("shares"),
        payload.get("price"),
        payload.get("acquired_disposed"),
        payload.get("direct_or_indirect"),
        payload.get("is_derivative"),
    ]
    normalized = "|".join("" if item is None else str(item).strip().lower() for item in parts)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def document_hash(xml_text: str | bytes) -> str:
    raw = xml_text if isinstance(xml_text, bytes) else xml_text.encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _owner_relationship(owner: ET.Element | None) -> dict[str, Any]:
    relationship = _child(owner, "reportingOwnerRelationship") if owner is not None else None
    return {
        "is_director": _bool_text(_text(relationship, "isDirector")),
        "is_officer": _bool_text(_text(relationship, "isOfficer")),
        "is_ten_percent_owner": _bool_text(_text(relationship, "isTenPercentOwner")),
        "is_other": _bool_text(_text(relationship, "isOther")),
        "officer_title": _text(relationship, "officerTitle"),
    }


def _footnotes(transaction: ET.Element) -> list[str]:
    refs: list[str] = []
    for element in transaction.iter():
        tag = _strip_namespace(element.tag)
        if tag in {"footnoteId", "footnote"}:
            value = element.attrib.get("id") or element.attrib.get("footnoteId") or (element.text or "").strip()
            if value:
                refs.append(value)
    return refs


def _transaction_from_element(
    element: ET.Element,
    *,
    is_derivative: bool,
    accession_number: str,
    issuer_cik: str | None,
    issuer_name: str | None,
    ticker_raw: str | None,
    ticker_normalized: str | None,
    owner_cik: str | None,
    owner_name: str | None,
    relationship: dict[str, Any],
    filing_date: date | None,
) -> dict[str, Any]:
    code = (_text(element, "transactionCoding/transactionCode") or "").strip().upper() or None
    normalized_type, code_description = classify_form4_transaction_code(code)
    shares = _float_text(_text(element, "transactionAmounts/transactionShares/value"))
    price = _float_text(_text(element, "transactionAmounts/transactionPricePerShare/value"))
    transaction_date = _parse_date(_text(element, "transactionDate/value"))
    acquired_disposed = _text(element, "transactionAmounts/transactionAcquiredDisposedCode/value")
    shares_owned_following = _float_text(_text(element, "postTransactionAmounts/sharesOwnedFollowingTransaction/value"))
    direct_or_indirect = _text(element, "ownershipNature/directOrIndirectOwnership/value")
    ownership_nature = _text(element, "ownershipNature/natureOfOwnership/value")
    security_title = _text(element, "securityTitle/value")
    value = shares * price if shares is not None and price is not None else None
    payload = {
        "accession_number": accession_number,
        "issuer_cik": issuer_cik,
        "issuer_name": issuer_name,
        "ticker_raw": ticker_raw,
        "ticker_normalized": ticker_normalized,
        "reporting_owner_cik": owner_cik,
        "reporting_owner_name": owner_name,
        "owner_relationship_json": json.dumps(relationship, sort_keys=True),
        "officer_title": relationship.get("officer_title"),
        "is_director": bool(relationship.get("is_director")),
        "is_officer": bool(relationship.get("is_officer")),
        "is_ten_percent_owner": bool(relationship.get("is_ten_percent_owner")),
        "transaction_date": transaction_date,
        "filing_date": filing_date,
        "security_title": security_title,
        "transaction_code": code,
        "transaction_code_description": code_description,
        "transaction_type_normalized": normalized_type,
        "shares": shares,
        "price": price,
        "value": value,
        "acquired_disposed": acquired_disposed,
        "shares_owned_following": shares_owned_following,
        "direct_or_indirect": direct_or_indirect,
        "ownership_nature": ownership_nature,
        "is_derivative": is_derivative,
        "footnotes_json": json.dumps(_footnotes(element), sort_keys=True),
        "ten_b5_1_flag": False,
        "parser_confidence": 0.95 if code and transaction_date else 0.65,
    }
    payload["normalized_hash"] = insider_transaction_hash(payload)
    return payload


def parse_form4_xml(
    xml_text: str | bytes,
    *,
    accession_number: str | None = None,
    source_url: str | None = None,
    xml_url: str | None = None,
    db: Session | None = None,
) -> dict[str, Any]:
    raw_text = xml_text.decode("utf-8", errors="replace") if isinstance(xml_text, bytes) else xml_text
    root = ET.fromstring(raw_text)
    issuer = _child(root, "issuer")
    owner = _child(root, "reportingOwner")
    owner_id = _child(owner, "reportingOwnerId") if owner is not None else None
    issuer_cik = _normalize_cik(_text(issuer, "issuerCik"))
    issuer_name = _text(issuer, "issuerName")
    issuer_symbol = _text(issuer, "issuerTradingSymbol")
    ticker_normalized, symbol_status = normalize_form4_symbol(issuer_symbol, issuer_name, db)
    owner_cik = _normalize_cik(_text(owner_id, "rptOwnerCik"))
    owner_name = _text(owner_id, "rptOwnerName")
    relationship = _owner_relationship(owner)
    filing_date = _parse_date(_text(root, "periodOfReport"))
    accession = accession_number or _text(root, "accessionNumber") or document_hash(raw_text)[:20]
    transactions: list[dict[str, Any]] = []
    for table_name, is_derivative in (("nonDerivativeTable", False), ("derivativeTable", True)):
        table = _child(root, table_name)
        if table is None:
            continue
        item_name = "derivativeTransaction" if is_derivative else "nonDerivativeTransaction"
        for transaction in _children(table, item_name):
            transactions.append(
                _transaction_from_element(
                    transaction,
                    is_derivative=is_derivative,
                    accession_number=accession,
                    issuer_cik=issuer_cik,
                    issuer_name=issuer_name,
                    ticker_raw=issuer_symbol,
                    ticker_normalized=ticker_normalized,
                    owner_cik=owner_cik,
                    owner_name=owner_name,
                    relationship=relationship,
                    filing_date=filing_date,
                )
            )
    ten_b5_1 = bool(re.search(r"10b5-?1", raw_text, flags=re.IGNORECASE))
    if ten_b5_1:
        for transaction in transactions:
            transaction["ten_b5_1_flag"] = True
    return {
        "filing": {
            "accession_number": accession,
            "issuer_cik": issuer_cik,
            "issuer_name": issuer_name,
            "issuer_trading_symbol": issuer_symbol,
            "ticker_normalized": ticker_normalized,
            "symbol_resolution_status": symbol_status,
            "reporting_owner_cik": owner_cik,
            "reporting_owner_name": owner_name,
            "filing_date": filing_date,
            "source_url": source_url,
            "xml_url": xml_url,
            "document_hash": document_hash(raw_text),
            "parser_version": FORM4_PARSER_VERSION,
            "parser_confidence": min([item["parser_confidence"] for item in transactions], default=0.0),
        },
        "transactions": transactions,
    }


def stage_form4_shadow(
    db: Session,
    *,
    xml_text: str | bytes,
    accession_number: str | None = None,
    source_url: str | None = None,
    xml_url: str | None = None,
) -> dict[str, Any]:
    parsed = parse_form4_xml(xml_text, accession_number=accession_number, source_url=source_url, xml_url=xml_url, db=db)
    filing_payload = parsed["filing"]
    filing = db.execute(
        select(SecForm4Filing)
        .where(SecForm4Filing.accession_number == filing_payload["accession_number"])
    ).scalar_one_or_none()
    if filing is None:
        filing = SecForm4Filing(
            accession_number=filing_payload["accession_number"],
            issuer_cik=filing_payload["issuer_cik"],
            issuer_name=filing_payload["issuer_name"],
            issuer_trading_symbol=filing_payload["issuer_trading_symbol"],
            reporting_owner_cik=filing_payload["reporting_owner_cik"],
            reporting_owner_name=filing_payload["reporting_owner_name"],
            filing_date=filing_payload["filing_date"],
            source_url=source_url,
            xml_url=xml_url,
            document_hash=filing_payload["document_hash"],
            raw_metadata_json=json.dumps({k: v for k, v in filing_payload.items() if k != "document_hash"}, sort_keys=True, default=str),
            raw_xml_text=xml_text.decode("utf-8", errors="replace") if isinstance(xml_text, bytes) else xml_text,
            parser_status="parsed",
            parser_version=FORM4_PARSER_VERSION,
            parser_confidence=filing_payload["parser_confidence"],
            parsed_at=datetime.now(timezone.utc),
        )
        db.add(filing)
        db.flush()

    inserted = skipped = 0
    for item in parsed["transactions"]:
        existing = db.execute(
            select(InsiderTransactionNormalized)
            .where(InsiderTransactionNormalized.normalized_hash == item["normalized_hash"])
        ).scalar_one_or_none()
        if existing is not None:
            skipped += 1
            continue
        db.add(InsiderTransactionNormalized(form4_filing_id=filing.id, **item))
        inserted += 1
    db.flush()
    return {
        "accession_number": filing.accession_number,
        "parsed": len(parsed["transactions"]),
        "inserted": inserted,
        "skipped": skipped,
    }


def promote_form4_shadow_events(db: Session, *, limit: int = 100) -> dict[str, int]:
    rows = db.execute(
        select(InsiderTransactionNormalized)
        .where(InsiderTransactionNormalized.is_duplicate.is_(False))
        .where(InsiderTransactionNormalized.ticker_normalized.is_not(None))
        .where(InsiderTransactionNormalized.transaction_type_normalized.in_(("open_market_purchase", "open_market_sale")))
        .order_by(InsiderTransactionNormalized.id.asc())
        .limit(limit)
    ).scalars().all()
    inserted = skipped = 0
    for row in rows:
        existing = db.execute(
            select(Event.id)
            .where(Event.source_provider == "sec_edgar")
            .where(Event.source_filing_id == row.normalized_hash)
            .limit(1)
        ).scalar_one_or_none()
        if existing is not None:
            skipped += 1
            continue
        event_date = row.filing_date or row.transaction_date or datetime.now(timezone.utc).date()
        event_dt = datetime(event_date.year, event_date.month, event_date.day, tzinfo=timezone.utc)
        trade_type = "purchase" if row.transaction_type_normalized == "open_market_purchase" else "sale"
        estimated_value = int(round(row.value)) if row.value is not None else None
        payload = {
            "external_id": f"sec_form4:{row.normalized_hash}",
            "source_provider": "sec_edgar",
            "accession_number": row.accession_number,
            "issuer_cik": row.issuer_cik,
            "reporting_cik": row.reporting_owner_cik,
            "insider_name": row.reporting_owner_name,
            "transaction_code": row.transaction_code,
            "transaction_type": row.transaction_type_normalized,
            "trade_type_canonical": trade_type,
            "is_market_trade": True,
            "transaction_date": row.transaction_date.isoformat() if row.transaction_date else None,
            "filing_date": row.filing_date.isoformat() if row.filing_date else None,
            "shares": row.shares,
            "price": row.price,
            "value": row.value,
            "parser_version": FORM4_PARSER_VERSION,
            "normalized_hash": row.normalized_hash,
            "shadow_promoted": True,
        }
        db.add(
            Event(
                event_type="insider_trade",
                ts=event_dt,
                event_date=event_dt,
                symbol=row.ticker_normalized,
                source="sec_edgar",
                trade_type=trade_type,
                transaction_type=row.transaction_code,
                amount_min=estimated_value,
                amount_max=estimated_value,
                impact_score=0.0,
                payload_json=json.dumps(payload, sort_keys=True),
                data_source="insider",
                source_provider="sec_edgar",
                source_filing_id=row.normalized_hash,
                parser_version=FORM4_PARSER_VERSION,
                provider_priority=10,
            )
        )
        inserted += 1
    db.flush()
    return {"inserted": inserted, "skipped": skipped}
