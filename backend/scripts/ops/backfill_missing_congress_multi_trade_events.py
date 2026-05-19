from __future__ import annotations

import argparse
import csv
import difflib
import hashlib
import json
import logging
import re
import sys
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.backfill_events_from_trades import (  # noqa: E402
    _build_backfill_id,
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
from app.models import Event, Filing, Member, Security, Transaction  # noqa: E402
from app.services.congress_metadata import get_congress_metadata_resolver  # noqa: E402
from app.utils.symbols import canonical_symbol  # noqa: E402
from sqlalchemy import select  # noqa: E402


logger = logging.getLogger(__name__)
DEFAULT_CONGRESS_BACKFILL_ARTIFACT_DIR = "/data/artifacts/congress_backfill"
KNOWN_TRANSACTION_TYPES = {
    "buy",
    "purchase",
    "sale",
    "sale (full)",
    "sale (partial)",
    "sell",
    "exchange",
}
GENERIC_SECURITY_LABELS = {
    "common stock",
    "class a shares",
    "class b shares",
    "stock option",
    "stock option right to buy",
    "option",
    "rsu",
    "restricted stock unit",
    "warrant",
    "ordinary shares",
    "units",
}
GENERIC_SECURITY_SUFFIXES = (
    " common stock",
    " class a common stock",
    " class b common stock",
    " class a shares",
    " class b shares",
    " ordinary shares",
    " stock option",
    " restricted stock unit",
    " units",
)
REVIEWED_ISSUER_ALIASES = {
    "cvs health corporation": "CVS",
    "jpmorgan chase and co": "JPM",
    "jp morgan chase and co": "JPM",
}
PDF_ROW_NOISE = {
    "asset",
    "asset description",
    "owner",
    "type",
    "date",
    "transaction date",
    "notification date",
    "amount",
    "capital gains",
    "gains",
    "gains >",
    "description",
    "periodic transaction report",
    "periodic transaction report for",
    "certification and signature",
    "initial public offering",
    "transaction",
    "transactions",
    "none",
    "state/district",
    "f s new",
    "fs new",
}


def _safe_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _parse_payload_json(payload_json: str | None) -> dict:
    try:
        payload = json.loads(payload_json or "{}")
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


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


def _row_asset_description(row: dict[str, Any]) -> str | None:
    return _safe_str(
        row.get("assetDescription")
        or row.get("asset_description")
        or row.get("asset")
        or row.get("company")
        or row.get("issuer")
        or row.get("issuerName")
        or row.get("security_name")
    )


def _parse_symbols(value: str | None) -> set[str]:
    if not value:
        return set()
    return {
        symbol
        for symbol in (canonical_symbol(part.strip()) for part in value.split(","))
        if symbol
    }


def _parse_date_arg(value: str | None) -> date | None:
    if not value:
        return None
    return datetime.strptime(value, "%Y-%m-%d").date()


def _parse_source_date(value: Any) -> date | None:
    if not value:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        text = value.strip()
        for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y"):
            try:
                return datetime.strptime(text[:10], fmt).date()
            except Exception:
                continue
    return None


def _source_amount_range(value: Any) -> tuple[float | None, float | None]:
    if value is None:
        return None, None
    if isinstance(value, (int, float)):
        amount = float(value)
        return amount, amount
    text = str(value).replace(",", "").replace("$", "").strip()
    if "-" in text:
        left, right = [part.strip() for part in text.split("-", 1)]
        try:
            lo = float(left) if left else None
        except Exception:
            lo = None
        try:
            hi = float(right) if right else None
        except Exception:
            hi = None
        return lo, hi
    try:
        amount = float(text)
    except Exception:
        return None, None
    return amount, amount


def _row_trade_date(row: dict[str, Any]) -> date | None:
    return _parse_source_date(row.get("transactionDate") or row.get("tradeDate") or row.get("transaction_date"))


def _row_report_date(row: dict[str, Any]) -> date | None:
    return _parse_source_date(row.get("disclosureDate") or row.get("reportDate") or row.get("filingDate"))


def _row_transaction_type(row: dict[str, Any]) -> str | None:
    value = _safe_str(row.get("type") or row.get("transactionType") or row.get("transaction_type"))
    return value.lower() if value else None


def _row_amount_range(row: dict[str, Any]) -> tuple[float | None, float | None]:
    return _source_amount_range(row.get("amount") or row.get("amountRange") or row.get("amount_range"))


def _date_key(value: date | None) -> str:
    return value.isoformat() if value else "unknown"


def _month_key(value: date | None) -> str:
    return value.isoformat()[:7] if value else "unknown"


def _member_name(member: Member) -> str:
    return f"{member.first_name or ''} {member.last_name or ''}".strip() or member.bioguide_id


def _document_id(document_url: str | None, document_hash: str | None) -> str | None:
    if document_url:
        path = urlparse(document_url).path if "://" in document_url else document_url
        name = Path(path.rstrip("/")).name
        if name:
            return name
    return document_hash


def _days_between(start: date | None, end: date | None) -> int | None:
    if start is None or end is None:
        return None
    return (end - start).days


def _normalize_issuer_text(value: str | None) -> str | None:
    text = _safe_str(value)
    if not text:
        return None
    text = text.replace("&", " and ")
    text = re.sub(r"\([^)]*\)", " ", text)
    text = re.sub(r"[^A-Za-z0-9]+", " ", text).strip().lower()
    text = re.sub(r"\s+", " ", text)
    if not text:
        return None
    for suffix in GENERIC_SECURITY_SUFFIXES:
        if text.endswith(suffix):
            text = text[: -len(suffix)].strip()
    return text or None


def _is_generic_security_label(value: str | None) -> bool:
    normalized = _normalize_issuer_text(value)
    if not normalized:
        return True
    return normalized in GENERIC_SECURITY_LABELS


def _candidate_issuer_strings(item: dict[str, Any]) -> list[str]:
    values = [
        item.get("security_name"),
        item.get("description"),
        item.get("raw_asset_description"),
        item.get("raw_issuer"),
        item.get("raw_company"),
    ]
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = _safe_str(value)
        key = _normalize_issuer_text(text)
        if not text or not key or key in seen or _is_generic_security_label(text):
            continue
        seen.add(key)
        result.append(text)
    return result


def _add_unique_mapping(mapping: dict[str, set[str]], raw_name: str | None, symbol: str | None) -> None:
    key = _normalize_issuer_text(raw_name)
    normalized_symbol = canonical_symbol(symbol)
    if not key or not normalized_symbol or _is_generic_security_label(raw_name):
        return
    mapping.setdefault(key, set()).add(normalized_symbol)


def _build_issuer_resolution_maps(db) -> tuple[dict[str, set[str]], dict[str, set[str]], dict[str, str]]:
    canonical: dict[str, set[str]] = {}
    historical: dict[str, set[str]] = {}
    for security in db.execute(select(Security)).scalars():
        _add_unique_mapping(canonical, security.name, security.symbol)

    event_rows = db.execute(
        select(Event.symbol, Event.payload_json)
        .where(Event.event_type == "congress_trade")
        .where(Event.symbol.is_not(None))
    ).all()
    for symbol, payload_json in event_rows:
        payload = _parse_payload_json(payload_json)
        raw_payload = payload.get("raw") if isinstance(payload.get("raw"), dict) else {}
        for value in (
            payload.get("issuer_name"),
            payload.get("issuerName"),
            payload.get("company_name"),
            payload.get("companyName"),
            payload.get("security_name"),
            payload.get("securityName"),
            raw_payload.get("issuer"),
            raw_payload.get("issuerName"),
            raw_payload.get("company"),
            raw_payload.get("assetDescription"),
            raw_payload.get("security_name"),
        ):
            _add_unique_mapping(historical, value, symbol)
    reviewed = {
        key: symbol
        for key, symbol in (
            (_normalize_issuer_text(alias), canonical_symbol(symbol))
            for alias, symbol in REVIEWED_ISSUER_ALIASES.items()
        )
        if key and symbol
    }
    return canonical, historical, reviewed


def _unique_symbol(mapping: dict[str, set[str]], key: str | None) -> str | None:
    if not key:
        return None
    symbols = mapping.get(key) or set()
    return next(iter(symbols)) if len(symbols) == 1 else None


def _resolve_candidate_ticker(
    item: dict[str, Any],
    *,
    canonical_map: dict[str, set[str]],
    historical_map: dict[str, set[str]],
    reviewed_alias_map: dict[str, str],
) -> dict[str, Any]:
    existing_symbol = canonical_symbol(item.get("symbol"))
    enriched_symbol = canonical_symbol(item.get("enriched_symbol"))
    issuer_strings = _candidate_issuer_strings(item)
    if existing_symbol:
        return {
            "resolved_symbol": existing_symbol,
            "resolution_confidence": "existing",
            "resolution_source": "candidate_symbol",
            "resolution_issuer": issuer_strings[0] if issuer_strings else item.get("security_name"),
            "resolution_score": 1.0,
            "issuer_candidates": issuer_strings,
        }
    if enriched_symbol:
        return {
            "resolved_symbol": enriched_symbol,
            "resolution_confidence": "source_exact",
            "resolution_source": "source_document_symbol",
            "resolution_issuer": issuer_strings[0] if issuer_strings else item.get("enriched_asset_description"),
            "resolution_score": 1.0,
            "issuer_candidates": issuer_strings,
        }
    if not issuer_strings:
        return {
            "resolved_symbol": None,
            "resolution_confidence": "unresolved",
            "resolution_source": "no_usable_issuer",
            "resolution_issuer": None,
            "resolution_score": None,
            "issuer_candidates": [],
        }

    for issuer in issuer_strings:
        key = _normalize_issuer_text(issuer)
        symbol = _unique_symbol(canonical_map, key)
        if symbol:
            return {
                "resolved_symbol": symbol,
                "resolution_confidence": "exact",
                "resolution_source": "security_name_exact",
                "resolution_issuer": issuer,
                "resolution_score": 1.0,
                "issuer_candidates": issuer_strings,
            }

    for issuer in issuer_strings:
        key = _normalize_issuer_text(issuer)
        symbol = _unique_symbol(historical_map, key)
        if symbol:
            return {
                "resolved_symbol": symbol,
                "resolution_confidence": "historical_exact",
                "resolution_source": "event_history_exact",
                "resolution_issuer": issuer,
                "resolution_score": 1.0,
                "issuer_candidates": issuer_strings,
            }

    for issuer in issuer_strings:
        key = _normalize_issuer_text(issuer)
        symbol = reviewed_alias_map.get(key or "")
        if symbol:
            return {
                "resolved_symbol": symbol,
                "resolution_confidence": "alias_reviewed",
                "resolution_source": "reviewed_alias",
                "resolution_issuer": issuer,
                "resolution_score": 1.0,
                "issuer_candidates": issuer_strings,
            }

    known_keys = sorted(set(canonical_map) | set(historical_map))
    best: tuple[float, str] | None = None
    for issuer in issuer_strings:
        key = _normalize_issuer_text(issuer)
        if not key:
            continue
        match = difflib.get_close_matches(key, known_keys, n=1, cutoff=0.96)
        if not match:
            continue
        score = difflib.SequenceMatcher(None, key, match[0]).ratio()
        if best is None or score > best[0]:
            best = (score, match[0])
    if best:
        symbol = _unique_symbol(canonical_map, best[1]) or _unique_symbol(historical_map, best[1])
        if symbol:
            return {
                "resolved_symbol": symbol,
                "resolution_confidence": "fuzzy_high",
                "resolution_source": "fuzzy_high",
                "resolution_issuer": best[1],
                "resolution_score": round(best[0], 4),
                "issuer_candidates": issuer_strings,
            }

    return {
        "resolved_symbol": None,
        "resolution_confidence": "unresolved",
        "resolution_source": "no_safe_match",
        "resolution_issuer": issuer_strings[0],
        "resolution_score": None,
        "issuer_candidates": issuer_strings,
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


def _event_identity_maps(db) -> tuple[set[str], set[int], set[str], Counter[str]]:
    external_ids, transaction_ids, backfill_ids = _existing_congress_event_identities(db)
    backfill_counts: Counter[str] = Counter()
    for (payload_json,) in db.execute(
        select(Event.payload_json).where(Event.event_type == "congress_trade")
    ):
        try:
            payload = json.loads(payload_json or "{}")
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue
        backfill_id = payload.get("backfill_id")
        if isinstance(backfill_id, str) and backfill_id.strip():
            backfill_counts[backfill_id.strip()] += 1
    return external_ids, transaction_ids, backfill_ids, backfill_counts


def _artifact_directory(artifact_dir: str | None) -> Path | None:
    if artifact_dir:
        return Path(artifact_dir)
    default = Path(DEFAULT_CONGRESS_BACKFILL_ARTIFACT_DIR)
    return default if default.exists() else None


def _latest_enrichment_detail_path(artifact_dir: str | None) -> Path | None:
    directory = _artifact_directory(artifact_dir)
    if directory is None or not directory.exists():
        return None
    matches = sorted(directory.glob("congress_backfill_enrichment_detail_*.csv"))
    return matches[-1] if matches else None


def _load_enrichment_map(artifact_dir: str | None = None) -> dict[int, dict[str, Any]]:
    path = _latest_enrichment_detail_path(artifact_dir)
    if path is None:
        return {}
    enrichments: dict[int, dict[str, Any]] = {}
    with path.open("r", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            tx_id_raw = _safe_str(row.get("transaction_id"))
            if not tx_id_raw or not tx_id_raw.isdigit():
                continue
            confidence = _safe_str(row.get("resolution_confidence"))
            if confidence not in {"source_exact", "exact", "historical_exact", "alias_reviewed", "fuzzy_high"}:
                continue
            enrichments[int(tx_id_raw)] = {
                "enrichment_artifact": str(path),
                "enrichment_source": _safe_str(row.get("enrichment_source")),
                "enrichment_status": _safe_str(row.get("enrichment_status")),
                "enriched_symbol": canonical_symbol(row.get("enriched_symbol")),
                "enriched_asset_description": _safe_str(row.get("enriched_asset_description")),
                "enriched_issuer": _safe_str(row.get("enriched_issuer")),
                "enriched_company": _safe_str(row.get("enriched_company")),
                "enriched_row_hash": _safe_str(row.get("enriched_row_hash")),
            }
    return enrichments


def _apply_candidate_enrichment(item: dict[str, Any], enrichment_by_tx: dict[int, dict[str, Any]]) -> None:
    enrichment = enrichment_by_tx.get(int(item["transaction_id"]))
    if not enrichment or enrichment.get("enrichment_status") != "recovered":
        return
    item.update(enrichment)
    recovered_asset = enrichment.get("enriched_asset_description")
    recovered_issuer = enrichment.get("enriched_issuer") or recovered_asset
    recovered_company = enrichment.get("enriched_company") or recovered_asset
    if recovered_asset and not _is_generic_security_label(recovered_asset):
        item["raw_asset_description"] = item.get("raw_asset_description") or recovered_asset
        item["description"] = item.get("description") or recovered_asset
    if recovered_issuer and not _is_generic_security_label(recovered_issuer):
        item["raw_issuer"] = item.get("raw_issuer") or recovered_issuer
        item["security_name"] = item.get("security_name") or recovered_issuer
    if recovered_company and not _is_generic_security_label(recovered_company):
        item["raw_company"] = item.get("raw_company") or recovered_company


def _existing_events_by_backfill_id(db) -> dict[str, list[dict[str, Any]]]:
    by_id: dict[str, list[dict[str, Any]]] = defaultdict(list)
    rows = db.execute(
        select(Event.id, Event.symbol, Event.member_name, Event.member_bioguide_id, Event.trade_type, Event.transaction_type, Event.amount_min, Event.amount_max, Event.payload_json)
        .where(Event.event_type == "congress_trade")
    ).all()
    for event_id, symbol, member_name, member_bioguide_id, trade_type, transaction_type, amount_min, amount_max, payload_json in rows:
        payload = _parse_payload_json(payload_json)
        backfill_id = payload.get("backfill_id")
        if not isinstance(backfill_id, str) or not backfill_id.strip():
            continue
        by_id[backfill_id.strip()].append(
            {
                "event_id": event_id,
                "symbol": symbol,
                "member": member_name,
                "member_bioguide_id": member_bioguide_id,
                "trade_type": trade_type,
                "transaction_type": transaction_type,
                "amount_min": amount_min,
                "amount_max": amount_max,
                "trade_date": payload.get("trade_date"),
                "report_date": payload.get("report_date"),
                "filing_id": payload.get("filing_id"),
                "document_url": payload.get("document_url"),
                "transaction_id": payload.get("transaction_id"),
            }
        )
    return by_id


def _candidate_shape_key(item: dict[str, Any]) -> tuple:
    return (
        item.get("source"),
        item.get("member_bioguide_id"),
        item.get("symbol"),
        item.get("transaction_type"),
        item.get("trade_date"),
        item.get("report_date"),
        item.get("amount_min"),
        item.get("amount_max"),
    )


def _candidate_source_row_hash(item: dict[str, Any]) -> str:
    key_fields = {
        "source": item.get("source"),
        "document_url": item.get("document_url"),
        "document_hash": item.get("document_hash"),
        "member_bioguide_id": item.get("member_bioguide_id"),
        "symbol": item.get("symbol") or item.get("resolved_symbol"),
        "issuer": _normalize_issuer_text(item.get("resolution_issuer") or item.get("security_name")),
        "description": _normalize_issuer_text(item.get("description") or item.get("raw_asset_description")),
        "transaction_type": item.get("transaction_type"),
        "trade_date": item.get("trade_date"),
        "report_date": item.get("report_date"),
        "amount_min": item.get("amount_min"),
        "amount_max": item.get("amount_max"),
        "owner_type": item.get("owner_type"),
    }
    normalized = json.dumps(key_fields, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _collision_reason(item: dict[str, Any], existing_events: list[dict[str, Any]]) -> str:
    if not existing_events:
        return "backfill_id_seen_without_loaded_event"
    for event in existing_events:
        same_document = event.get("document_url") and event.get("document_url") == item.get("document_url")
        same_filing = event.get("filing_id") and event.get("filing_id") == item.get("filing_id")
        if same_document or same_filing:
            return "same_document_or_filing_duplicate"
    if not item.get("symbol"):
        return "collision_with_missing_ticker_candidate"
    return "same_member_symbol_side_trade_date_amount_different_document"


def _candidate_risk(item: dict[str, Any], shape_count: int) -> tuple[str, list[str]]:
    issues: list[tuple[str, str]] = []
    symbol = item.get("symbol") or item.get("resolved_symbol")
    issuer = item.get("security_name") or item.get("resolution_issuer")
    tx_type = str(item.get("transaction_type") or "").strip().lower()
    amount_min = item.get("amount_min")
    amount_max = item.get("amount_max")
    stale_days = item.get("report_trade_lag_days")
    confidence = item.get("resolution_confidence")

    if not item.get("trade_date"):
        issues.append(("high", "missing_trade_date"))
    if not item.get("report_date"):
        issues.append(("high", "missing_report_date"))
    if not symbol and not issuer:
        issues.append(("high", "missing_ticker_and_issuer"))
    elif not item.get("symbol") and confidence == "fuzzy_high":
        issues.append(("medium", "ticker_resolved_by_fuzzy_high_requires_review"))
    elif not item.get("symbol") and confidence == "unresolved":
        issues.append(("high", "missing_ticker_unresolved"))
    if shape_count > 1:
        issues.append(("high", "duplicate_like_transaction_shape"))
    if item.get("backfill_collision"):
        issues.append(("high", "backfill_identity_matches_existing_event"))
    if amount_min is None or amount_max is None:
        issues.append(("medium", "missing_or_ambiguous_amount"))
    if tx_type not in KNOWN_TRANSACTION_TYPES:
        issues.append(("medium", "unusual_transaction_type"))
    if not symbol and issuer:
        issues.append(("medium", "missing_ticker_with_issuer"))
    if stale_days is not None and stale_days > 365:
        issues.append(("medium", "trade_date_more_than_365_days_before_report"))

    if any(severity == "high" for severity, _issue in issues):
        return "high", [issue for _severity, issue in issues]
    if issues:
        return "medium", [issue for _severity, issue in issues]
    return "low", []


def _build_missing_event_candidates(
    db,
    *,
    enrichment_by_tx: dict[int, dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    external_ids, transaction_ids, backfill_ids, backfill_counts = _event_identity_maps(db)
    existing_events_by_backfill = _existing_events_by_backfill_id(db)
    canonical_map, historical_map, reviewed_alias_map = _build_issuer_resolution_maps(db)
    enrichment_by_tx = enrichment_by_tx or {}
    rows = db.execute(
        select(Transaction, Filing, Member, Security)
        .join(Filing, Filing.id == Transaction.filing_id)
        .join(Member, Member.id == Transaction.member_id)
        .outerjoin(Security, Security.id == Transaction.security_id)
        .where(Filing.source.in_(("house_fmp", "senate_fmp")))
        .order_by(Transaction.report_date.desc().nullslast(), Transaction.id.desc())
    ).all()

    candidates: list[dict[str, Any]] = []
    for tx, filing, member, security in rows:
        payload = _congress_event_payload(tx, filing, member, security)
        if payload is None:
            payload = {
                "external_id": f"congress_tx:{tx.id}",
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
                "symbol": None,
                "security_name": security.name if security else None,
                "asset_class": security.asset_class if security else None,
                "member": {
                    "bioguide_id": member.bioguide_id,
                    "name": _member_name(member),
                    "chamber": member.chamber,
                    "party": member.party,
                    "state": member.state,
                },
                "source": filing.source,
                "filing_source": filing.source,
                "filing_date": filing.filing_date.isoformat() if filing.filing_date else None,
                "document_url": filing.document_url,
                "event_type": "congress_trade",
            }
            payload["backfill_id"] = _build_backfill_id(payload)
        external_id = str(payload["external_id"])
        backfill_id = str(payload["backfill_id"])
        has_matching_event = external_id in external_ids or tx.id in transaction_ids
        backfill_collision = backfill_id in backfill_ids
        if has_matching_event:
            continue
        report_trade_lag_days = _days_between(tx.trade_date, tx.report_date)
        item = {
            "transaction_id": tx.id,
            "filing_id": filing.id,
            "document_id": _document_id(filing.document_url, filing.document_hash),
            "document_hash": filing.document_hash,
            "document_url": filing.document_url,
            "source": filing.source,
            "member": _member_name(member),
            "member_bioguide_id": member.bioguide_id,
            "symbol": payload.get("symbol"),
            "security_name": security.name if security else None,
            "asset_class": security.asset_class if security else None,
            "raw_asset_description": tx.description,
            "raw_issuer": security.name if security else None,
            "raw_company": security.name if security else None,
            "transaction_type": tx.transaction_type,
            "trade_date": tx.trade_date.isoformat() if tx.trade_date else None,
            "report_date": tx.report_date.isoformat() if tx.report_date else None,
            "filing_date": filing.filing_date.isoformat() if filing.filing_date else None,
            "amount_min": tx.amount_range_min,
            "amount_max": tx.amount_range_max,
            "owner_type": tx.owner_type,
            "description": tx.description,
            "external_id": external_id,
            "backfill_id": backfill_id,
            "has_matching_event": has_matching_event,
            "backfill_collision": backfill_collision,
            "existing_backfill_event_count": backfill_counts.get(backfill_id, 0),
            "report_trade_lag_days": report_trade_lag_days,
            "event_action": "skip_duplicate_risk" if backfill_collision else "insert_event",
        }
        _apply_candidate_enrichment(item, enrichment_by_tx)
        item.update(
            _resolve_candidate_ticker(
                item,
                canonical_map=canonical_map,
                historical_map=historical_map,
                reviewed_alias_map=reviewed_alias_map,
            )
        )
        collision_events = existing_events_by_backfill.get(backfill_id, [])
        item["collision_existing_events"] = collision_events[:5]
        item["collision_reason"] = (
            _collision_reason(item, collision_events)
            if backfill_collision
            else None
        )
        item["source_row_hash"] = _candidate_source_row_hash(item)
        item["source_row_index_available"] = False
        candidates.append(item)

    shape_counts = Counter(_candidate_shape_key(item) for item in candidates)
    for item in candidates:
        risk, issues = _candidate_risk(item, shape_counts[_candidate_shape_key(item)])
        item["risk"] = risk
        item["risk_issues"] = issues
    return candidates


def _filter_candidates(
    candidates: list[dict[str, Any]],
    *,
    risk: str | None = None,
    since_report_date: date | None = None,
    until_report_date: date | None = None,
    member: str | None = None,
    source: str | None = None,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    filtered = candidates
    if risk:
        allowed = {part.strip().lower() for part in risk.split(",") if part.strip()}
        filtered = [item for item in filtered if item.get("risk") in allowed]
    if since_report_date:
        filtered = [
            item
            for item in filtered
            if item.get("report_date") and date.fromisoformat(item["report_date"]) >= since_report_date
        ]
    if until_report_date:
        filtered = [
            item
            for item in filtered
            if item.get("report_date") and date.fromisoformat(item["report_date"]) <= until_report_date
        ]
    if member:
        term = member.strip().lower()
        filtered = [item for item in filtered if term in str(item.get("member") or "").lower()]
    if source:
        source_value = f"{source}_fmp" if source in {"house", "senate"} else source
        filtered = [item for item in filtered if item.get("source") == source_value]
    if limit:
        filtered = filtered[:limit]
    return filtered


def _source_name_from_candidate_source(source: str | None) -> str | None:
    if source == "house_fmp":
        return "house"
    if source == "senate_fmp":
        return "senate"
    return source


def _row_matches_candidate(row: dict[str, Any], item: dict[str, Any]) -> bool:
    if not _row_matches_document(row, item.get("document_url") or item.get("document_id") or ""):
        return False
    row_trade_date = _row_trade_date(row)
    if row_trade_date and item.get("trade_date") and row_trade_date.isoformat() != item.get("trade_date"):
        return False
    row_type = _row_transaction_type(row)
    item_type = str(item.get("transaction_type") or "").strip().lower()
    if row_type and item_type and row_type != item_type:
        return False
    row_lo, row_hi = _row_amount_range(row)
    if row_lo is not None and item.get("amount_min") is not None and float(row_lo) != float(item["amount_min"]):
        return False
    if row_hi is not None and item.get("amount_max") is not None and float(row_hi) != float(item["amount_max"]):
        return False
    return True


def _source_row_hash(row: dict[str, Any]) -> str:
    normalized = json.dumps(row, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _enrichment_from_source_row(row: dict[str, Any], *, source: str) -> dict[str, Any] | None:
    symbol = _row_symbol(row)
    asset = _row_asset_description(row)
    if not symbol and (not asset or _is_generic_security_label(asset)):
        return None
    return {
        "enrichment_status": "recovered",
        "enrichment_source": f"{source}_source_row",
        "enriched_symbol": symbol,
        "enriched_asset_description": asset,
        "enriched_issuer": asset,
        "enriched_company": asset,
        "enriched_row_hash": _source_row_hash(row),
    }


def _fetch_source_rows_for_candidates(
    candidates: list[dict[str, Any]],
    *,
    pages: int,
    limit: int,
) -> dict[int, dict[str, Any]]:
    if not candidates or pages <= 0:
        return {}
    tokens_by_tx = {
        int(item["transaction_id"]): _document_tokens(item.get("document_url") or item.get("document_id"))
        for item in candidates
    }
    found: dict[int, dict[str, Any]] = {}
    wanted_sources = sorted({_source_name_from_candidate_source(item.get("source")) for item in candidates})
    for source in wanted_sources:
        if source not in {"house", "senate"}:
            continue
        fetch_page = fetch_house_page if source == "house" else fetch_senate_page
        source_candidates = [
            item for item in candidates if _source_name_from_candidate_source(item.get("source")) == source
        ]
        for page in range(pages):
            try:
                rows = fetch_page(page=page, limit=limit)
            except Exception as exc:
                logger.warning("Source-row enrichment fetch failed for %s page=%s: %s", source, page, exc)
                break
            if not rows:
                break
            for row in rows:
                row_tokens = _document_tokens(_row_document_url(row))
                if not row_tokens:
                    continue
                for item in source_candidates:
                    tx_id = int(item["transaction_id"])
                    if tx_id in found or not (tokens_by_tx[tx_id] & row_tokens):
                        continue
                    if not _row_matches_candidate(row, item):
                        continue
                    enrichment = _enrichment_from_source_row(row, source=source)
                    if enrichment:
                        found[tx_id] = enrichment
    return found


def _extract_pdf_text(document_url: str) -> str:
    response = requests.get(document_url, timeout=45)
    response.raise_for_status()
    try:
        from pypdf import PdfReader
    except Exception as exc:  # pragma: no cover - depends on deployed optional dependency
        raise RuntimeError("PDF enrichment requires pypdf to be installed in the backend image.") from exc
    reader = PdfReader(BytesIO(response.content))
    return "\n".join(page.extract_text() or "" for page in reader.pages)


def _date_text_variants(value: str | None) -> set[str]:
    parsed = _parse_source_date(value)
    if not parsed:
        return set()
    return {
        parsed.isoformat(),
        f"{parsed.month}/{parsed.day}/{parsed.year}",
        f"{parsed.month:02d}/{parsed.day:02d}/{parsed.year}",
    }


def _amount_text_variants(lo: Any, hi: Any) -> set[str]:
    values = set()
    for amount in (lo, hi):
        if amount is None:
            continue
        try:
            number = int(float(amount))
        except Exception:
            continue
        values.add(str(number))
        values.add(f"{number:,}")
        values.add(f"${number:,}")
    return values


def _line_has_any(line: str, needles: set[str]) -> bool:
    lower = line.lower()
    return any(needle.lower() in lower for needle in needles if needle)


def _clean_pdf_text_line(line: str) -> str:
    text = line.replace("\x00", " ")
    text = re.sub(r"\s+", " ", text).strip()
    text = re.sub(r"\b([A-Z])(?:\s+)(?=[A-Z]\b)", r"\1", text)
    return re.sub(r"\s+", " ", text).strip()


def _explicit_ticker_from_text(line: str) -> str | None:
    match = re.search(r"\bticker\s*[:\-]\s*([A-Z][A-Z0-9.\-]{0,7})\b", line, flags=re.IGNORECASE)
    return canonical_symbol(match.group(1)) if match else None


def _looks_like_pdf_issuer_line(line: str, item: dict[str, Any]) -> bool:
    text = _clean_pdf_text_line(line)
    normalized = _normalize_issuer_text(text)
    if not text or not normalized or normalized in PDF_ROW_NOISE or _is_generic_security_label(text):
        return False
    if len(text) < 3 or len(text) > 160:
        return False
    if _line_has_any(text, _date_text_variants(item.get("trade_date"))):
        return False
    if _line_has_any(text, _amount_text_variants(item.get("amount_min"), item.get("amount_max"))):
        return False
    lowered = text.lower()
    noisy_fragments = [
        "periodic transaction report",
        "honorable",
        "state/district",
        "state district",
        "status",
        "amount",
        "owner",
        "transaction date",
        "notification",
        "capital gains",
        "gains >",
        "f s:",
        "f s new",
        "fs:",
        "fs new",
        "signature",
        "filed",
    ]
    if any(fragment in lowered for fragment in noisy_fragments):
        return False
    if ":" in text:
        label = text.split(":", 1)[0].strip().lower()
        if len(label) <= 18 and not any(word in lowered for word in ("inc", "corp", "llc", "l.p", " lp", "fund", "trust")):
            return False
    tx_type = str(item.get("transaction_type") or "").lower()
    if tx_type and lowered == tx_type:
        return False
    if not re.search(r"[A-Za-z]{3,}", text):
        return False
    return True


def _enrichment_from_pdf_text(text: str, item: dict[str, Any]) -> dict[str, Any] | None:
    lines = [_clean_pdf_text_line(line) for line in text.splitlines()]
    lines = [line for line in lines if line]
    if not lines:
        return None
    date_variants = _date_text_variants(item.get("trade_date"))
    amount_variants = _amount_text_variants(item.get("amount_min"), item.get("amount_max"))
    tx_type = str(item.get("transaction_type") or "").lower()
    best_window: list[str] = []
    best_score = -1
    for idx, line in enumerate(lines):
        if not _line_has_any(line, date_variants):
            continue
        start = max(0, idx - 8)
        end = min(len(lines), idx + 9)
        window = lines[start:end]
        window_text = " ".join(window).lower()
        score = 3
        if amount_variants and _line_has_any(window_text, amount_variants):
            score += 2
        if tx_type and tx_type in window_text:
            score += 1
        if score > best_score:
            best_score = score
            best_window = window
    if not best_window:
        return None

    direct_symbol = None
    for line in best_window:
        direct_symbol = direct_symbol or _explicit_ticker_from_text(line)
    issuer_candidates = [line for line in best_window if _looks_like_pdf_issuer_line(line, item)]
    if not direct_symbol and not issuer_candidates:
        return None
    issuer = issuer_candidates[0] if issuer_candidates else None
    return {
        "enrichment_status": "recovered",
        "enrichment_source": "pdf_text",
        "enriched_symbol": direct_symbol,
        "enriched_asset_description": issuer,
        "enriched_issuer": issuer,
        "enriched_company": issuer,
        "enriched_row_hash": hashlib.sha256(" ".join(best_window).encode("utf-8")).hexdigest(),
    }


def _fetch_pdf_enrichments(candidates: list[dict[str, Any]]) -> dict[int, dict[str, Any]]:
    by_document: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in candidates:
        document_url = _safe_str(item.get("document_url"))
        if document_url and document_url.lower().endswith(".pdf"):
            by_document[document_url].append(item)

    found: dict[int, dict[str, Any]] = {}
    for document_url, items in by_document.items():
        try:
            text = _extract_pdf_text(document_url)
        except Exception as exc:
            logger.warning("PDF enrichment failed for %s: %s", document_url, exc)
            continue
        for item in items:
            enrichment = _enrichment_from_pdf_text(text, item)
            if enrichment:
                found[int(item["transaction_id"])] = enrichment
    return found


def _enriched_candidate_preview(
    item: dict[str, Any],
    enrichment: dict[str, Any] | None,
    *,
    canonical_map: dict[str, set[str]],
    historical_map: dict[str, set[str]],
    reviewed_alias_map: dict[str, str],
) -> dict[str, Any]:
    preview = dict(item)
    if enrichment:
        preview.update(enrichment)
        _apply_candidate_enrichment(preview, {int(item["transaction_id"]): enrichment})
    preview.update(
        _resolve_candidate_ticker(
            preview,
            canonical_map=canonical_map,
            historical_map=historical_map,
            reviewed_alias_map=reviewed_alias_map,
        )
    )
    risk, issues = _candidate_risk(preview, 1)
    preview["risk_after_enrichment"] = risk
    preview["risk_issues_after_enrichment"] = issues
    return preview


def _write_enrichment_artifacts(
    *,
    summary: dict[str, Any],
    rows: list[dict[str, Any]],
    artifact_dir: str | None,
) -> dict[str, str]:
    directory = Path(artifact_dir or DEFAULT_CONGRESS_BACKFILL_ARTIFACT_DIR)
    directory.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    summary_path = directory / f"congress_backfill_enrichment_summary_{stamp}.json"
    detail_path = directory / f"congress_backfill_enrichment_detail_{stamp}.csv"
    detail_json_path = directory / f"congress_backfill_enrichment_detail_{stamp}.json"
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    detail_json_path.write_text(json.dumps(rows, indent=2, sort_keys=True), encoding="utf-8")
    fieldnames = [
        "transaction_id",
        "filing_id",
        "document_id",
        "document_url",
        "source",
        "member",
        "trade_date",
        "report_date",
        "transaction_type",
        "amount_min",
        "amount_max",
        "enrichment_status",
        "enrichment_source",
        "enriched_symbol",
        "enriched_asset_description",
        "enriched_issuer",
        "enriched_company",
        "enriched_row_hash",
        "resolved_symbol",
        "resolution_confidence",
        "resolution_source",
        "risk_after_enrichment",
        "risk_issues_after_enrichment",
    ]
    with detail_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for item in rows:
            row = {key: item.get(key) for key in fieldnames}
            row["risk_issues_after_enrichment"] = "|".join(item.get("risk_issues_after_enrichment") or [])
            writer.writerow(row)
    return {
        "summary_json": str(summary_path),
        "detail_csv": str(detail_path),
        "detail_json": str(detail_json_path),
    }


def run_enrich_unresolved(
    *,
    artifact_dir: str | None,
    limit: int | None,
    since_report_date: date | None,
    until_report_date: date | None,
    source: str | None,
    document: str | None,
    pages: int,
    page_limit: int,
) -> dict[str, Any]:
    db = SessionLocal()
    try:
        base_candidates = _build_missing_event_candidates(db)
        candidates = [
            item
            for item in base_candidates
            if item.get("risk") == "high"
            and item.get("resolution_confidence") == "unresolved"
            and (not document or item.get("document_id") == document or document in _document_tokens(item.get("document_url")))
        ]
        candidates = _filter_candidates(
            candidates,
            risk=None,
            since_report_date=since_report_date,
            until_report_date=until_report_date,
            member=None,
            source=source,
            limit=limit,
        )
        source_enrichments = _fetch_source_rows_for_candidates(candidates, pages=pages, limit=page_limit)
        remaining = [item for item in candidates if int(item["transaction_id"]) not in source_enrichments]
        pdf_enrichments = _fetch_pdf_enrichments(remaining)
        enrichments = {**source_enrichments, **pdf_enrichments}

        canonical_map, historical_map, reviewed_alias_map = _build_issuer_resolution_maps(db)
        rows = [
            _enriched_candidate_preview(
                item,
                enrichments.get(int(item["transaction_id"])),
                canonical_map=canonical_map,
                historical_map=historical_map,
                reviewed_alias_map=reviewed_alias_map,
            )
            for item in candidates
        ]
        recovered_rows = [row for row in rows if row.get("enrichment_status") == "recovered"]
        summary = {
            "mode": "enrich-unresolved",
            "dry_run": True,
            "total_candidates_before_filter": len(base_candidates),
            "selected_candidates": len(candidates),
            "documents_inspected": len({item.get("document_url") or item.get("document_id") for item in candidates}),
            "rows_reparsed": len(rows),
            "issuer_recovered_count": sum(1 for row in recovered_rows if row.get("enriched_issuer")),
            "asset_description_recovered_count": sum(1 for row in recovered_rows if row.get("enriched_asset_description")),
            "ticker_recovered_count": sum(1 for row in recovered_rows if row.get("enriched_symbol")),
            "still_unresolved_count": sum(1 for row in rows if row.get("resolution_confidence") == "unresolved"),
            "confidence_breakdown": Counter(row.get("resolution_confidence") for row in rows).most_common(),
            "risk_after_enrichment": Counter(row.get("risk_after_enrichment") for row in rows).most_common(),
            "recovered_examples": recovered_rows[:20],
            "still_unresolved_examples": [
                row for row in rows if row.get("resolution_confidence") == "unresolved"
            ][:20],
        }
        artifacts = _write_enrichment_artifacts(summary=summary, rows=rows, artifact_dir=artifact_dir)
        return {"mode": "enrich-unresolved", "summary": summary, "artifacts": artifacts}
    finally:
        db.close()


def _candidate_summary(candidates: list[dict[str, Any]]) -> dict[str, Any]:
    affected_documents = {
        (item.get("source"), item.get("document_url") or item.get("document_hash"))
        for item in candidates
    }
    report_dates = [
        date.fromisoformat(item["report_date"])
        for item in candidates
        if item.get("report_date")
    ]
    trade_dates = [
        date.fromisoformat(item["trade_date"])
        for item in candidates
        if item.get("trade_date")
    ]
    duplicate_groups = Counter(_candidate_shape_key(item) for item in candidates)
    duplicate_group_count = sum(1 for count in duplicate_groups.values() if count > 1)
    resolution_counts = Counter(item.get("resolution_confidence") for item in candidates)
    safely_resolved_confidences = {"source_exact", "exact", "historical_exact", "alias_reviewed"}
    missing_ticker_resolved = sum(
        1
        for item in candidates
        if not item.get("symbol") and item.get("resolution_confidence") in safely_resolved_confidences
    )
    fuzzy_resolved = sum(1 for item in candidates if item.get("resolution_confidence") == "fuzzy_high")
    return {
        "total_candidate_events": len(candidates),
        "affected_filings_or_documents": len(affected_documents),
        "date_range": {
            "report_min": min(report_dates).isoformat() if report_dates else None,
            "report_max": max(report_dates).isoformat() if report_dates else None,
            "trade_min": min(trade_dates).isoformat() if trade_dates else None,
            "trade_max": max(trade_dates).isoformat() if trade_dates else None,
        },
        "by_source": Counter(item.get("source") or "unknown" for item in candidates).most_common(),
        "by_risk": Counter(item.get("risk") for item in candidates).most_common(),
        "top_affected_members": Counter(item.get("member") or "unknown" for item in candidates).most_common(25),
        "top_tickers": Counter(item.get("symbol") or "missing" for item in candidates).most_common(25),
        "rows_by_report_month": Counter(_month_key(date.fromisoformat(item["report_date"])) if item.get("report_date") else "unknown" for item in candidates).most_common(),
        "rows_by_trade_month": Counter(_month_key(date.fromisoformat(item["trade_date"])) if item.get("trade_date") else "unknown" for item in candidates).most_common(),
        "stale_trade_vs_report_gt_365d": sum(
            1 for item in candidates if (item.get("report_trade_lag_days") or 0) > 365
        ),
        "already_having_matching_events": sum(1 for item in candidates if item.get("has_matching_event")),
        "missing_events": sum(1 for item in candidates if not item.get("has_matching_event")),
        "missing_ticker": sum(1 for item in candidates if not item.get("symbol")),
        "unresolved_issuer_company": sum(1 for item in candidates if not item.get("symbol") and not item.get("security_name")),
        "questionable_amount_side_date": sum(1 for item in candidates if item.get("risk") in {"medium", "high"}),
        "duplicate_risk_groups": duplicate_group_count,
        "backfill_identity_collision_rows": sum(1 for item in candidates if item.get("backfill_collision")),
        "resolution_confidence_counts": resolution_counts.most_common(),
        "missing_ticker_safely_resolved": missing_ticker_resolved,
        "missing_ticker_fuzzy_high": fuzzy_resolved,
        "unresolved_no_usable_issuer": sum(
            1 for item in candidates if item.get("resolution_source") == "no_usable_issuer"
        ),
        "unresolved_no_safe_match": sum(
            1 for item in candidates if item.get("resolution_source") == "no_safe_match"
        ),
    }


def _diagnostic_reports(candidates: list[dict[str, Any]]) -> dict[str, Any]:
    issuer_counter: Counter[str] = Counter()
    asset_counter: Counter[str] = Counter()
    exact_matches = []
    historical_matches = []
    fuzzy_matches = []
    no_safe_match = []
    for item in candidates:
        issuer_candidates = item.get("issuer_candidates") or []
        confidence = item.get("resolution_confidence")
        if confidence == "unresolved":
            if not issuer_candidates:
                issuer_counter["<missing>"] += 1
            for issuer in issuer_candidates:
                issuer_counter[issuer] += 1
            asset = item.get("raw_asset_description") or item.get("security_name") or "<missing>"
            asset_counter[asset] += 1
        if confidence == "exact":
            exact_matches.append(item)
        elif confidence == "historical_exact":
            historical_matches.append(item)
        elif confidence == "fuzzy_high":
            fuzzy_matches.append(item)
        elif confidence == "unresolved":
            no_safe_match.append(item)

    collisions_by_reason = Counter(item.get("collision_reason") or "none" for item in candidates if item.get("backfill_collision"))
    collision_samples = [
        {
            "transaction_id": item.get("transaction_id"),
            "backfill_id": item.get("backfill_id"),
            "collision_reason": item.get("collision_reason"),
            "candidate": {
                "member": item.get("member"),
                "symbol": item.get("symbol"),
                "resolved_symbol": item.get("resolved_symbol"),
                "document_id": item.get("document_id"),
                "source_row_hash": item.get("source_row_hash"),
                "source_row_index_available": item.get("source_row_index_available"),
                "trade_date": item.get("trade_date"),
                "report_date": item.get("report_date"),
                "transaction_type": item.get("transaction_type"),
                "amount_min": item.get("amount_min"),
                "amount_max": item.get("amount_max"),
            },
            "existing_events": item.get("collision_existing_events") or [],
            "recommendation": (
                "safe_duplicate_skip"
                if item.get("collision_reason") == "same_document_or_filing_duplicate"
                else "manual_review_required"
            ),
        }
        for item in candidates
        if item.get("backfill_collision")
    ][:25]

    grouped: defaultdict[tuple, list[dict[str, Any]]] = defaultdict(list)
    for item in candidates:
        grouped[_candidate_shape_key(item)].append(item)
    duplicate_groups = []
    for key, items in grouped.items():
        if len(items) <= 1:
            continue
        row_hashes = sorted({item.get("source_row_hash") for item in items if item.get("source_row_hash")})
        duplicate_groups.append(
            {
                "shape_key": list(key),
                "count": len(items),
                "distinct_source_row_hashes": row_hashes,
                "row_key_refinement_possible": len(row_hashes) > 1,
                "rows": [
                    {
                        "transaction_id": item.get("transaction_id"),
                        "filing_id": item.get("filing_id"),
                        "document_id": item.get("document_id"),
                        "source_row_hash": item.get("source_row_hash"),
                        "source_row_index_available": item.get("source_row_index_available"),
                        "member": item.get("member"),
                        "symbol": item.get("symbol"),
                        "resolved_symbol": item.get("resolved_symbol"),
                        "trade_date": item.get("trade_date"),
                        "report_date": item.get("report_date"),
                        "transaction_type": item.get("transaction_type"),
                        "amount_min": item.get("amount_min"),
                        "amount_max": item.get("amount_max"),
                    }
                    for item in items
                ],
                "recommendation": (
                    "needs_row_key_refinement"
                    if len(row_hashes) > 1
                    else "manual_review_required"
                ),
            }
        )

    def _compact(items: list[dict[str, Any]], limit: int = 25) -> list[dict[str, Any]]:
        return [
            {
                "transaction_id": item.get("transaction_id"),
                "member": item.get("member"),
                "document_id": item.get("document_id"),
                "issuer": item.get("resolution_issuer"),
                "symbol": item.get("symbol"),
                "resolved_symbol": item.get("resolved_symbol"),
                "confidence": item.get("resolution_confidence"),
                "source": item.get("resolution_source"),
                "score": item.get("resolution_score"),
            }
            for item in items[:limit]
        ]

    return {
        "top_unresolved_issuer_strings": issuer_counter.most_common(50),
        "top_unresolved_asset_descriptions": asset_counter.most_common(50),
        "exact_company_ticker_matches": _compact(exact_matches),
        "historical_exact_matches": _compact(historical_matches),
        "fuzzy_high_matches": _compact(fuzzy_matches),
        "no_safe_match_samples": _compact(no_safe_match),
        "collision_summary": collisions_by_reason.most_common(),
        "collision_samples": collision_samples,
        "duplicate_risk_groups": duplicate_groups,
        "row_disambiguation": {
            "source_row_index_available": False,
            "source_row_hash_available": True,
            "source_row_hash_fields": [
                "source",
                "document_url",
                "document_hash",
                "member_bioguide_id",
                "resolved symbol",
                "issuer",
                "description",
                "transaction_type",
                "trade_date",
                "report_date",
                "amount_min",
                "amount_max",
                "owner_type",
            ],
        },
    }


def _apply_resolved_symbol_to_event(event: Event, item: dict[str, Any]) -> None:
    if event.symbol or not item.get("resolved_symbol"):
        return
    if item.get("resolution_confidence") not in {"source_exact", "exact", "historical_exact", "alias_reviewed"}:
        return
    payload = _parse_payload_json(event.payload_json)
    payload["symbol"] = item["resolved_symbol"]
    payload["resolved_symbol"] = item["resolved_symbol"]
    payload["ticker_resolution"] = {
        "confidence": item.get("resolution_confidence"),
        "source": item.get("resolution_source"),
        "issuer": item.get("resolution_issuer"),
        "score": item.get("resolution_score"),
    }
    if not payload.get("security_name") and item.get("resolution_issuer"):
        payload["security_name"] = item["resolution_issuer"]
    event.symbol = item["resolved_symbol"]
    event.payload_json = json.dumps(payload, sort_keys=True)


def _write_audit_artifacts(
    *,
    summary: dict[str, Any],
    diagnostics: dict[str, Any],
    candidates: list[dict[str, Any]],
    artifact_dir: str | None,
) -> dict[str, str]:
    if not artifact_dir:
        return {}
    directory = Path(artifact_dir)
    directory.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    summary_path = directory / f"congress_backfill_audit_summary_{stamp}.json"
    diagnostics_path = directory / f"congress_backfill_audit_diagnostics_{stamp}.json"
    detail_path = directory / f"congress_backfill_audit_detail_{stamp}.csv"
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    diagnostics_path.write_text(json.dumps(diagnostics, indent=2, sort_keys=True), encoding="utf-8")
    fieldnames = [
        "risk",
        "risk_issues",
        "resolved_symbol",
        "resolution_confidence",
        "resolution_source",
        "resolution_issuer",
        "resolution_score",
        "source_row_hash",
        "source_row_index_available",
        "enrichment_artifact",
        "enrichment_source",
        "enrichment_status",
        "enriched_symbol",
        "enriched_asset_description",
        "enriched_issuer",
        "transaction_id",
        "filing_id",
        "document_id",
        "source",
        "member",
        "member_bioguide_id",
        "symbol",
        "security_name",
        "transaction_type",
        "trade_date",
        "report_date",
        "amount_min",
        "amount_max",
        "report_trade_lag_days",
        "backfill_collision",
        "collision_reason",
        "event_action",
        "document_url",
    ]
    with detail_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for item in candidates:
            row = {key: item.get(key) for key in fieldnames}
            row["risk_issues"] = "|".join(item.get("risk_issues") or [])
            writer.writerow(row)
    return {
        "summary_json": str(summary_path),
        "diagnostics_json": str(diagnostics_path),
        "detail_csv": str(detail_path),
    }


def run_candidate_audit(*, artifact_dir: str | None = None) -> dict[str, Any]:
    db = SessionLocal()
    try:
        enrichment_by_tx = _load_enrichment_map(artifact_dir)
        candidates = _build_missing_event_candidates(db, enrichment_by_tx=enrichment_by_tx)
        summary = _candidate_summary(candidates)
        diagnostics = _diagnostic_reports(candidates)
        artifacts = _write_audit_artifacts(
            summary=summary,
            diagnostics=diagnostics,
            candidates=candidates,
            artifact_dir=artifact_dir,
        )
        return {
            "mode": "audit",
            "enrichment_rows_loaded": len(enrichment_by_tx),
            "summary": summary,
            "diagnostics": diagnostics,
            "artifacts": artifacts,
            "sample_candidates": candidates[:25],
        }
    finally:
        db.close()


def run_candidate_batch(
    *,
    apply: bool,
    risk: str | None,
    since_report_date: date | None,
    until_report_date: date | None,
    member: str | None,
    source: str | None,
    limit: int | None,
    artifact_dir: str | None = None,
) -> dict[str, Any]:
    mode = "apply" if apply else "dry-run"
    db = SessionLocal()
    try:
        enrichment_by_tx = _load_enrichment_map(artifact_dir)
        all_candidates = _build_missing_event_candidates(db, enrichment_by_tx=enrichment_by_tx)
        selected = _filter_candidates(
            all_candidates,
            risk=risk,
            since_report_date=since_report_date,
            until_report_date=until_report_date,
            member=member,
            source=source,
            limit=limit,
        )
        if apply:
            high_risk = [item for item in selected if item.get("risk") == "high"]
            if high_risk:
                raise RuntimeError(f"Refusing to apply {len(high_risk)} high-risk candidates.")

        external_ids, transaction_ids, backfill_ids, _backfill_counts = _event_identity_maps(db)
        rows = []
        events_to_insert = 0
        events_inserted = 0
        duplicate_skips = 0
        risk_counts = Counter(item.get("risk") for item in selected)
        resolution_counts = Counter(item.get("resolution_confidence") for item in selected)

        for item in selected:
            duplicate = (
                item["external_id"] in external_ids
                or item["transaction_id"] in transaction_ids
                or item["backfill_id"] in backfill_ids
            )
            action = "skip_duplicate" if duplicate else "insert_event"
            event_id = None
            if duplicate:
                duplicate_skips += 1
            else:
                events_to_insert += 1
                if apply:
                    row = db.execute(
                        select(Transaction, Filing, Member, Security)
                        .join(Filing, Filing.id == Transaction.filing_id)
                        .join(Member, Member.id == Transaction.member_id)
                        .outerjoin(Security, Security.id == Transaction.security_id)
                        .where(Transaction.id == item["transaction_id"])
                    ).one_or_none()
                    if row is None:
                        raise RuntimeError(f"Transaction {item['transaction_id']} disappeared during apply.")
                    tx, filing, row_member, security = row
                    event = _congress_event_from_transaction(tx, filing, row_member, security)
                    _apply_resolved_symbol_to_event(event, item)
                    db.add(event)
                    db.flush()
                    event_id = event.id
                    external_ids.add(item["external_id"])
                    transaction_ids.add(item["transaction_id"])
                    backfill_ids.add(item["backfill_id"])
                    events_inserted += 1
            rows.append(
                {
                    **item,
                    "action": action,
                    "event_id": event_id,
                }
            )

        if apply:
            db.commit()
        else:
            db.rollback()

        return {
            "mode": mode,
            "scope": "candidate_batch",
            "filters": {
                "risk": risk,
                "since_report_date": since_report_date.isoformat() if since_report_date else None,
                "until_report_date": until_report_date.isoformat() if until_report_date else None,
                "member": member,
                "source": source,
                "limit": limit,
            },
            "total_candidates_before_filter": len(all_candidates),
            "enrichment_rows_loaded": len(enrichment_by_tx),
            "selected_count": len(selected),
            "selected_by_risk": risk_counts.most_common(),
            "selected_by_resolution_confidence": resolution_counts.most_common(),
            "events_to_insert": events_to_insert,
            "events_inserted": events_inserted,
            "duplicate_skips": duplicate_skips,
            "rows": rows[:100],
        }
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


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
    risk: str | None = None,
    since_report_date: date | None = None,
    until_report_date: date | None = None,
    member: str | None = None,
    source: str | None = None,
    artifact_dir: str | None = None,
) -> dict:
    filtered_batch = any([risk, since_report_date, until_report_date, member, source])
    if filtered_batch:
        return run_candidate_batch(
            apply=apply,
            risk=risk,
            since_report_date=since_report_date,
            until_report_date=until_report_date,
            member=member,
            source=source,
            limit=limit,
            artifact_dir=artifact_dir,
        )

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
    parser.add_argument("--audit-candidates", action="store_true", help="Report missing-event candidates with risk buckets.")
    parser.add_argument("--enrich-unresolved", action="store_true", help="Dry-run reparse unresolved source documents into audit artifacts.")
    parser.add_argument("--artifact-dir", help="Directory for audit JSON/CSV artifacts.")
    parser.add_argument("--member", help="Optional targeted member sanity filter.")
    parser.add_argument("--symbols", help="Optional comma-separated symbol sanity filter.")
    parser.add_argument("--source", choices=["house", "senate"], help="Optional source hint for targeted document lookup.")
    parser.add_argument("--house", action="store_true", help="Filter candidate batch to House rows.")
    parser.add_argument("--senate", action="store_true", help="Filter candidate batch to Senate rows.")
    parser.add_argument("--risk", help="Comma-separated risk bucket filter for candidate batch, e.g. low or low,medium.")
    parser.add_argument("--since-report-date", help="Candidate batch lower report-date bound, YYYY-MM-DD.")
    parser.add_argument("--until-report-date", help="Candidate batch upper report-date bound, YYYY-MM-DD.")
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
    batch_source = None
    if args.house and args.senate:
        parser.error("Use at most one of --house or --senate.")
    if args.house:
        batch_source = "house"
    elif args.senate:
        batch_source = "senate"
    elif not args.document:
        batch_source = args.source

    batch_filtered = any(
        [
            args.risk,
            args.since_report_date,
            args.until_report_date,
            args.member,
            batch_source,
        ]
    )
    if apply and not args.document and not args.all and not batch_filtered:
        parser.error("--apply requires --document, filtered batch flags, or --all --i-understand-this-is-broad.")

    if args.audit_candidates:
        result = run_candidate_audit(artifact_dir=args.artifact_dir)
        print(json.dumps(result, indent=2, sort_keys=True))
        return

    if args.enrich_unresolved:
        if apply:
            parser.error("--enrich-unresolved is artifact-only; use --dry-run or omit mode flags.")
        result = run_enrich_unresolved(
            artifact_dir=args.artifact_dir,
            limit=args.limit,
            since_report_date=_parse_date_arg(args.since_report_date),
            until_report_date=_parse_date_arg(args.until_report_date),
            source=batch_source,
            document=args.document,
            pages=args.pages,
            page_limit=args.limit,
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return

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
            risk=args.risk,
            since_report_date=_parse_date_arg(args.since_report_date),
            until_report_date=_parse_date_arg(args.until_report_date),
            member=args.member,
            source=batch_source,
            artifact_dir=args.artifact_dir,
        )
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
