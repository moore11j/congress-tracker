from __future__ import annotations

import hashlib
import json
import re
from datetime import date, datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import CongressDisclosureFiling, CongressTransactionNormalized, Event, SymbolResolutionOverride
from app.services.congress_assets import classify_congress_disclosure_asset
from app.utils.symbols import canonical_symbol

CONGRESS_PARSER_VERSION = "official_congress_v1"


def _as_str(value: Any) -> str | None:
    if value is None:
        return None
    parsed = str(value).strip()
    return parsed if parsed else None


def _parse_date(value: Any) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        cleaned = value.strip()
        if not cleaned:
            return None
        for candidate in (cleaned[:10], cleaned):
            try:
                return datetime.fromisoformat(candidate.replace("Z", "+00:00")).date()
            except Exception:
                pass
        for fmt in ("%m/%d/%Y", "%m/%d/%y"):
            try:
                return datetime.strptime(cleaned, fmt).date()
            except Exception:
                pass
    return None


def congress_source_hash(payload: dict[str, Any] | str | bytes) -> str:
    if isinstance(payload, bytes):
        raw = payload
    elif isinstance(payload, str):
        raw = payload.encode("utf-8")
    else:
        raw = json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def normalize_congress_owner(value: str | None) -> str:
    normalized = (value or "").strip().lower()
    if not normalized:
        return "unknown"
    if "spouse" in normalized:
        return "spouse"
    if "dependent" in normalized or "child" in normalized:
        return "dependent"
    if "joint" in normalized:
        return "joint"
    if normalized in {"self", "member", "owner"}:
        return "self"
    return "unknown"


def normalize_congress_transaction_type(value: str | None) -> str | None:
    normalized = (value or "").strip().lower()
    if not normalized:
        return None
    if normalized in {"p", "purchase", "buy", "bought"} or normalized.startswith("purchase"):
        return "purchase"
    if normalized in {"s", "sale", "sell", "sold"} or normalized.startswith("sale"):
        return "sale"
    if "exchange" in normalized:
        return "exchange"
    if "partial" in normalized and "sale" in normalized:
        return "sale"
    return normalized


def parse_amount_range(value: Any) -> tuple[float | None, float | None]:
    if value is None or value == "":
        return None, None
    if isinstance(value, (int, float)):
        parsed = float(value)
        return parsed, parsed
    cleaned = str(value).replace("$", "").replace(",", "").strip()
    if not cleaned:
        return None, None
    if "over" in cleaned.lower():
        numbers = re.findall(r"\d+(?:\.\d+)?", cleaned)
        return (float(numbers[0]), None) if numbers else (None, None)
    numbers = [float(item) for item in re.findall(r"\d+(?:\.\d+)?", cleaned)]
    if not numbers:
        return None, None
    if len(numbers) == 1:
        return numbers[0], numbers[0]
    return numbers[0], numbers[1]


def normalize_congress_symbol(raw_symbol: str | None, issuer_name: str | None = None, db: Session | None = None) -> tuple[str | None, str]:
    raw = (raw_symbol or "").strip()
    if db is not None:
        override = db.execute(
            select(SymbolResolutionOverride)
            .where(SymbolResolutionOverride.domain_key.in_(("congress_trades", "house_disclosures", "senate_disclosures")))
            .where(SymbolResolutionOverride.raw_symbol == (raw or None))
            .where(SymbolResolutionOverride.issuer_name == (issuer_name or None))
            .limit(1)
        ).scalar_one_or_none()
        if override:
            return override.normalized_symbol, "admin_override" if override.normalized_symbol else "admin_unresolved"

    if not raw or raw in {"--", "N/A", "n/a"}:
        return None, "unresolved"
    upper = raw.upper().strip()
    share_class_variants = {
        "BRK.B": "BRK-B",
        "BRK/B": "BRK-B",
        "BF.B": "BF-B",
        "BF/B": "BF-B",
    }
    if upper in share_class_variants:
        return share_class_variants[upper], "resolved"
    if "." in upper:
        base, suffix = upper.split(".", 1)
        if len(suffix) == 1 and suffix.isalpha() and 1 <= len(base) <= 5:
            return f"{base}-{suffix}", "resolved"
    if "/" in upper:
        base, suffix = upper.split("/", 1)
        if len(suffix) == 1 and suffix.isalpha() and 1 <= len(base) <= 5:
            return f"{base}-{suffix}", "resolved"
    symbol = canonical_symbol(upper)
    return (symbol, "resolved") if symbol else (None, "unresolved")


def classify_congress_asset_type(
    *,
    raw_symbol: str | None,
    security_name: str | None,
    issuer_name: str | None,
    asset_type: str | None,
) -> str:
    classification = classify_congress_disclosure_asset(
        security_description=security_name or issuer_name,
        asset_class=asset_type,
        raw_symbol=raw_symbol,
    )
    if classification:
        if classification.instrument_type == "treasury":
            return "treasury"
        if classification.instrument_type == "crypto":
            return "crypto"
        return classification.instrument_type or classification.asset_class or "non_equity"
    text = " ".join([asset_type or "", security_name or "", issuer_name or ""]).lower()
    if "treasury" in text or "t-bill" in text or "bill" in text and "u.s" in text:
        return "treasury"
    if "exchange traded fund" in text or " etf" in text:
        return "etf"
    if (raw_symbol or "").strip():
        return "stock"
    if "private" in text:
        return "private"
    return "unresolved"


def congress_transaction_hash(payload: dict[str, Any]) -> str:
    parts = [
        payload.get("filing_id"),
        payload.get("chamber"),
        payload.get("member_id") or payload.get("member_name_raw"),
        payload.get("owner_normalized") or payload.get("owner_raw"),
        payload.get("transaction_date"),
        payload.get("issuer_name_raw") or payload.get("security_name_raw"),
        payload.get("ticker_normalized") or payload.get("ticker_raw"),
        payload.get("transaction_type_normalized") or payload.get("transaction_type_raw"),
        payload.get("amount_range_raw") or f"{payload.get('amount_low')}:{payload.get('amount_high')}",
        payload.get("amendment_flag"),
        payload.get("parser_version") or CONGRESS_PARSER_VERSION,
    ]
    normalized = "|".join("" if item is None else str(item).strip().lower() for item in parts)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def normalize_congress_transaction(
    row: dict[str, Any],
    *,
    chamber: str,
    source_provider: str,
    filing_id: str | None = None,
    filing_date: date | None = None,
    source_url: str | None = None,
    document_url: str | None = None,
    db: Session | None = None,
) -> dict[str, Any]:
    member_name = _as_str(
        row.get("member_name")
        or row.get("memberName")
        or row.get("representative")
        or row.get("senator")
        or row.get("office")
        or row.get("name")
    )
    member_id = _as_str(row.get("member_id") or row.get("memberId") or row.get("bioguide_id") or row.get("bioguideId"))
    owner_raw = _as_str(row.get("owner") or row.get("owner_type") or row.get("ownerType"))
    tx_date = _parse_date(row.get("transaction_date") or row.get("transactionDate") or row.get("trade_date") or row.get("date"))
    disclosure_date = _parse_date(row.get("disclosure_date") or row.get("disclosureDate") or row.get("report_date") or row.get("filingDate")) or filing_date
    ticker_raw = _as_str(row.get("ticker") or row.get("symbol"))
    issuer_name = _as_str(row.get("issuer_name") or row.get("issuerName") or row.get("assetDescription") or row.get("company"))
    security_name = _as_str(row.get("security_name") or row.get("securityName") or row.get("asset") or row.get("description"))
    asset_type_raw = _as_str(row.get("asset_type") or row.get("assetType") or row.get("asset_class"))
    transaction_type_raw = _as_str(row.get("transaction_type") or row.get("transactionType") or row.get("type"))
    amount_range_raw = _as_str(row.get("amount") or row.get("amount_range") or row.get("amountRange"))
    amount_low, amount_high = parse_amount_range(amount_range_raw)
    ticker_normalized, symbol_status = normalize_congress_symbol(ticker_raw, issuer_name, db)
    asset_type_normalized = classify_congress_asset_type(
        raw_symbol=ticker_raw,
        security_name=security_name,
        issuer_name=issuer_name,
        asset_type=asset_type_raw,
    )
    if asset_type_normalized != "stock" and symbol_status == "unresolved":
        symbol_status = asset_type_normalized
    normalized = {
        "filing_id": filing_id or _as_str(row.get("filing_id") or row.get("filingId") or row.get("document_id") or row.get("documentId")),
        "source_provider": source_provider,
        "chamber": chamber,
        "member_name_raw": member_name,
        "member_id": member_id,
        "owner_raw": owner_raw,
        "owner_normalized": normalize_congress_owner(owner_raw),
        "transaction_date": tx_date,
        "disclosure_date": disclosure_date,
        "ticker_raw": ticker_raw,
        "ticker_normalized": ticker_normalized,
        "issuer_name_raw": issuer_name,
        "security_name_raw": security_name,
        "asset_type_raw": asset_type_raw,
        "asset_type_normalized": asset_type_normalized,
        "transaction_type_raw": transaction_type_raw,
        "transaction_type_normalized": normalize_congress_transaction_type(transaction_type_raw),
        "amount_range_raw": amount_range_raw,
        "amount_low": amount_low,
        "amount_high": amount_high,
        "source_url": source_url or _as_str(row.get("source_url") or row.get("sourceUrl")),
        "document_url": document_url or _as_str(row.get("document_url") or row.get("documentUrl") or row.get("link") or row.get("pdf")),
        "source_line_ref": _as_str(row.get("source_line_ref") or row.get("line") or row.get("row")),
        "symbol_resolution_status": symbol_status,
        "parser_confidence": 0.9 if tx_date and (ticker_normalized or issuer_name or security_name) else 0.55,
        "parser_version": CONGRESS_PARSER_VERSION,
        "amendment_flag": bool(row.get("amendment") or row.get("amendment_flag") or row.get("isAmendment")),
    }
    normalized["normalized_hash"] = congress_transaction_hash(normalized)
    return normalized


def _transaction_rows(raw: dict[str, Any]) -> list[dict[str, Any]]:
    transactions = raw.get("transactions")
    if isinstance(transactions, list):
        return [item for item in transactions if isinstance(item, dict)]
    rows = raw.get("rows")
    if isinstance(rows, list):
        return [item for item in rows if isinstance(item, dict)]
    return [raw]


def parse_house_disclosure(raw: dict[str, Any], *, db: Session | None = None) -> list[dict[str, Any]]:
    filing_date = _parse_date(raw.get("filing_date") or raw.get("filingDate") or raw.get("disclosureDate"))
    filing_id = _as_str(raw.get("filing_id") or raw.get("filingId") or raw.get("document_id") or raw.get("documentId"))
    source_url = _as_str(raw.get("source_url") or raw.get("sourceUrl"))
    document_url = _as_str(raw.get("document_url") or raw.get("documentUrl") or raw.get("link") or raw.get("pdf"))
    return [
        normalize_congress_transaction(
            {**raw, **row},
            chamber="house",
            source_provider="official_house",
            filing_id=filing_id,
            filing_date=filing_date,
            source_url=source_url,
            document_url=document_url,
            db=db,
        )
        for row in _transaction_rows(raw)
    ]


def parse_senate_disclosure(raw: dict[str, Any], *, db: Session | None = None) -> list[dict[str, Any]]:
    filing_date = _parse_date(raw.get("filing_date") or raw.get("filingDate") or raw.get("disclosureDate"))
    filing_id = _as_str(raw.get("filing_id") or raw.get("filingId") or raw.get("document_id") or raw.get("documentId"))
    source_url = _as_str(raw.get("source_url") or raw.get("sourceUrl"))
    document_url = _as_str(raw.get("document_url") or raw.get("documentUrl") or raw.get("link") or raw.get("pdf"))
    return [
        normalize_congress_transaction(
            {**raw, **row},
            chamber="senate",
            source_provider="official_senate",
            filing_id=filing_id,
            filing_date=filing_date,
            source_url=source_url,
            document_url=document_url,
            db=db,
        )
        for row in _transaction_rows(raw)
    ]


def stage_congress_disclosure_shadow(
    db: Session,
    *,
    source_provider: str,
    chamber: str,
    raw: dict[str, Any],
) -> dict[str, Any]:
    parser = parse_house_disclosure if chamber == "house" else parse_senate_disclosure
    parsed = parser(raw, db=db)
    filing_id = _as_str(raw.get("filing_id") or raw.get("filingId") or raw.get("document_id") or raw.get("documentId")) or congress_source_hash(raw)[:16]
    filing = db.execute(
        select(CongressDisclosureFiling)
        .where(CongressDisclosureFiling.source_provider == source_provider)
        .where(CongressDisclosureFiling.filing_id == filing_id)
    ).scalar_one_or_none()
    if filing is None:
        filing = CongressDisclosureFiling(
            source_provider=source_provider,
            chamber=chamber,
            filing_id=filing_id,
            source_url=_as_str(raw.get("source_url") or raw.get("sourceUrl")),
            document_url=_as_str(raw.get("document_url") or raw.get("documentUrl") or raw.get("link") or raw.get("pdf")),
            document_hash=congress_source_hash(raw),
            member_name_raw=_as_str(raw.get("member_name") or raw.get("memberName") or raw.get("office")),
            member_id=_as_str(raw.get("member_id") or raw.get("memberId") or raw.get("bioguide_id")),
            filing_date=_parse_date(raw.get("filing_date") or raw.get("filingDate") or raw.get("disclosureDate")),
            report_type=_as_str(raw.get("report_type") or raw.get("reportType")),
            amendment_flag=bool(raw.get("amendment") or raw.get("amendment_flag")),
            raw_metadata_json=json.dumps(raw, sort_keys=True, default=str),
            parser_status="parsed",
            parser_version=CONGRESS_PARSER_VERSION,
            parser_confidence=min([item["parser_confidence"] for item in parsed], default=0.0),
            parsed_at=datetime.now(timezone.utc),
        )
        db.add(filing)
        db.flush()

    inserted = skipped = 0
    for item in parsed:
        existing = db.execute(
            select(CongressTransactionNormalized)
            .where(CongressTransactionNormalized.normalized_hash == item["normalized_hash"])
        ).scalar_one_or_none()
        if existing is not None:
            skipped += 1
            continue
        db.add(
            CongressTransactionNormalized(
                disclosure_filing_id=filing.id,
                **{key: value for key, value in item.items() if key not in {"parser_version", "amendment_flag"}},
            )
        )
        inserted += 1
    db.flush()
    return {"filing_id": filing_id, "parsed": len(parsed), "inserted": inserted, "skipped": skipped}


def promote_congress_shadow_events(db: Session, *, limit: int = 100) -> dict[str, int]:
    rows = db.execute(
        select(CongressTransactionNormalized)
        .where(CongressTransactionNormalized.is_duplicate.is_(False))
        .where(CongressTransactionNormalized.ticker_normalized.is_not(None))
        .order_by(CongressTransactionNormalized.id.asc())
        .limit(limit)
    ).scalars().all()
    inserted = skipped = 0
    for row in rows:
        existing = db.execute(
            select(Event.id)
            .where(Event.source_provider == row.source_provider)
            .where(Event.source_filing_id == row.normalized_hash)
            .limit(1)
        ).scalar_one_or_none()
        if existing is not None:
            skipped += 1
            continue
        event_date = row.disclosure_date or row.transaction_date or datetime.now(timezone.utc).date()
        event_dt = datetime(event_date.year, event_date.month, event_date.day, tzinfo=timezone.utc)
        payload = {
            "external_id": f"official_congress:{row.normalized_hash}",
            "source_provider": row.source_provider,
            "source_filing_id": row.filing_id,
            "document_url": row.document_url,
            "transaction_date": row.transaction_date.isoformat() if row.transaction_date else None,
            "disclosure_date": row.disclosure_date.isoformat() if row.disclosure_date else None,
            "owner_type": row.owner_normalized,
            "transaction_type": row.transaction_type_normalized,
            "amount_range_min": row.amount_low,
            "amount_range_max": row.amount_high,
            "symbol": row.ticker_normalized,
            "issuer_name": row.issuer_name_raw,
            "security_name": row.security_name_raw,
            "parser_version": CONGRESS_PARSER_VERSION,
            "normalized_hash": row.normalized_hash,
            "shadow_promoted": True,
        }
        db.add(
            Event(
                event_type="congress_trade",
                ts=event_dt,
                event_date=event_dt,
                symbol=row.ticker_normalized,
                source=row.source_provider,
                member_name=row.member_name_raw,
                member_bioguide_id=row.member_id,
                chamber=row.chamber,
                trade_type=row.transaction_type_normalized,
                transaction_type=row.transaction_type_raw,
                amount_min=int(row.amount_low) if row.amount_low is not None else None,
                amount_max=int(row.amount_high) if row.amount_high is not None else None,
                impact_score=0.0,
                payload_json=json.dumps(payload, sort_keys=True),
                data_source="congress",
                source_provider=row.source_provider,
                source_filing_id=row.normalized_hash,
                source_document_url=row.document_url,
                parser_version=CONGRESS_PARSER_VERSION,
                provider_priority=10,
            )
        )
        inserted += 1
    db.flush()
    return {"inserted": inserted, "skipped": skipped}
