from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sqlalchemy import select  # noqa: E402

from app.db import SessionLocal  # noqa: E402
from app.models import Event, Filing, Member, Security, TradeOutcome, Transaction  # noqa: E402
from app.services.congress_assets import CONGRESS_EQUITY_EVENT_TYPE  # noqa: E402
from app.services.member_performance import compute_congress_trade_outcomes  # noqa: E402
from app.utils.symbols import canonical_symbol  # noqa: E402
from scripts.ops.backfill_missing_congress_multi_trade_events import (  # noqa: E402
    _build_issuer_resolution_maps,
    _resolve_candidate_ticker,
)


BAD_EVENT_IDENTITY_LABELS = {
    "congress_trade",
    "congress_treasury_trade",
    "congress_crypto_trade",
    "insider_trade",
    "institutional_buy",
    "government_contract",
    "event",
    "security",
}
SAFE_CONFIDENCE = {"existing", "source_exact", "exact", "historical_exact", "alias_reviewed"}


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Repair recent malformed Congress event ticker/company identity.")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="Preview only. Default.")
    mode.add_argument("--apply", action="store_true", help="Write safe repairs.")
    parser.add_argument("--since-report-date", required=True, help="Lower report-date bound, YYYY-MM-DD.")
    parser.add_argument("--limit", type=int, default=500)
    return parser.parse_args()


def _parse_date(value: str | None) -> date | None:
    if not value:
        return None
    return datetime.strptime(value[:10], "%Y-%m-%d").date()


def _payload(payload_json: str | None) -> dict[str, Any]:
    try:
        parsed = json.loads(payload_json or "{}")
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _text(value: object | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _safe_symbol(value: object | None) -> str | None:
    symbol = canonical_symbol(_text(value))
    if not symbol or symbol.lower() in BAD_EVENT_IDENTITY_LABELS:
        return None
    return symbol


def _safe_label(value: object | None, symbol: str | None = None) -> str | None:
    text = _text(value)
    if not text or text.lower() in BAD_EVENT_IDENTITY_LABELS:
        return None
    if symbol and text.upper() == symbol.upper():
        return None
    return text


def _transaction_id(payload: dict[str, Any]) -> int | None:
    value = payload.get("transaction_id") or payload.get("transactionId")
    try:
        return int(value)
    except Exception:
        return None


def _report_date(event: Event, payload: dict[str, Any]) -> date | None:
    parsed = _parse_date(_text(payload.get("report_date") or payload.get("reportDate") or payload.get("filing_date")))
    if parsed:
        return parsed
    if event.event_date:
        return event.event_date.date()
    if event.ts:
        return event.ts.date()
    return None


def _display_label(payload: dict[str, Any], event: Event) -> str | None:
    return _text(
        payload.get("company_name")
        or payload.get("companyName")
        or payload.get("issuer_name")
        or payload.get("issuerName")
        or payload.get("security_name")
        or payload.get("securityName")
        or payload.get("security_description")
        or payload.get("securityDescription")
        or payload.get("description")
        or event.event_type
    )


def _needs_repair(event: Event, payload: dict[str, Any]) -> bool:
    current_symbol = _safe_symbol(event.symbol or payload.get("symbol") or payload.get("ticker"))
    visible_label = _display_label(payload, event)
    return (
        not current_symbol
        or _text(event.symbol or payload.get("symbol") or payload.get("ticker") or "").lower() in BAD_EVENT_IDENTITY_LABELS
        or (visible_label or "").strip().lower() in BAD_EVENT_IDENTITY_LABELS
    )


def _candidate_from_event(
    db,
    event: Event,
    payload: dict[str, Any],
    canonical_map: dict[str, set[str]],
    historical_map: dict[str, set[str]],
    reviewed_alias_map: dict[str, str],
) -> dict[str, Any]:
    tx = filing = member = security = None
    tx_id = _transaction_id(payload)
    if tx_id is not None:
        row = db.execute(
            select(Transaction, Filing, Member, Security)
            .join(Filing, Filing.id == Transaction.filing_id)
            .join(Member, Member.id == Transaction.member_id)
            .outerjoin(Security, Security.id == Transaction.security_id)
            .where(Transaction.id == tx_id)
        ).one_or_none()
        if row is not None:
            tx, filing, member, security = row

    issuer = _safe_label(security.name if security is not None else None)
    item = {
        "symbol": _safe_symbol(security.symbol if security is not None else None),
        "enriched_symbol": _safe_symbol(payload.get("ticker") or payload.get("symbol")),
        "security_name": issuer or _safe_label(payload.get("security_name") or payload.get("securityName")),
        "description": _safe_label((tx.description if tx is not None else None) or payload.get("description")),
        "raw_asset_description": _safe_label((tx.description if tx is not None else None) or payload.get("security_description") or payload.get("securityDescription")),
        "raw_issuer": issuer or _safe_label(payload.get("issuer_name") or payload.get("issuerName")),
        "raw_company": issuer or _safe_label(payload.get("company_name") or payload.get("companyName")),
    }
    resolution = _resolve_candidate_ticker(
        item,
        canonical_map=canonical_map,
        historical_map=historical_map,
        reviewed_alias_map=reviewed_alias_map,
    )
    symbol = _safe_symbol(resolution.get("resolved_symbol"))
    company = (
        _safe_label(security.name if security is not None else None, symbol)
        or _safe_label(resolution.get("resolution_issuer"), symbol)
        or _safe_label(item.get("security_name"), symbol)
    )
    confidence = resolution.get("resolution_confidence")
    safe = bool(symbol and confidence in SAFE_CONFIDENCE)
    skip_reason = None
    if not symbol:
        skip_reason = "unresolved_symbol"
    elif confidence not in SAFE_CONFIDENCE:
        skip_reason = f"unsafe_confidence:{confidence}"

    return {
        "event": event,
        "payload": payload,
        "transaction": tx,
        "filing": filing,
        "member": member,
        "security": security,
        "event_id": event.id,
        "transaction_id": tx_id,
        "member_name": event.member_name,
        "document_id": (Path(filing.document_url).name if filing is not None and filing.document_url else payload.get("document_url")),
        "report_date": _report_date(event, payload).isoformat() if _report_date(event, payload) else None,
        "trade_date": _text(payload.get("trade_date") or payload.get("transaction_date")),
        "side": event.trade_type or event.transaction_type or payload.get("transaction_type"),
        "amount_min": event.amount_min,
        "amount_max": event.amount_max,
        "old_symbol": event.symbol or payload.get("symbol") or payload.get("ticker"),
        "old_company": _display_label(payload, event),
        "new_symbol": symbol,
        "new_company": company,
        "confidence": confidence,
        "resolution_source": resolution.get("resolution_source"),
        "safe_to_apply": safe,
        "skip_reason": skip_reason,
        "would_refresh_outcome": safe,
        "raw_payload_fields": {
            "symbol": payload.get("symbol"),
            "ticker": payload.get("ticker"),
            "company_name": payload.get("company_name"),
            "issuer_name": payload.get("issuer_name"),
            "security_name": payload.get("security_name"),
            "security_description": payload.get("security_description"),
            "description": payload.get("description"),
        },
    }


def _apply_identity(candidate: dict[str, Any]) -> None:
    event: Event = candidate["event"]
    payload = dict(candidate["payload"])
    symbol = candidate["new_symbol"]
    company = candidate["new_company"]
    event.symbol = symbol
    event.event_type = CONGRESS_EQUITY_EVENT_TYPE
    if candidate.get("member") is not None:
        member: Member = candidate["member"]
        event.member_name = event.member_name or f"{member.first_name or ''} {member.last_name or ''}".strip()
        event.member_bioguide_id = event.member_bioguide_id or member.bioguide_id
        event.chamber = event.chamber or member.chamber
        event.party = event.party or member.party
    if candidate.get("transaction") is not None:
        tx: Transaction = candidate["transaction"]
        event.trade_type = event.trade_type or (tx.transaction_type or "").strip().lower()
        event.transaction_type = event.transaction_type or tx.transaction_type
        event.amount_min = event.amount_min if event.amount_min is not None else tx.amount_range_min
        event.amount_max = event.amount_max if event.amount_max is not None else tx.amount_range_max
    payload.update(
        {
            "symbol": symbol,
            "ticker": symbol,
            "company_name": company,
            "companyName": company,
            "issuer_name": company,
            "issuerName": company,
            "security_name": company,
            "securityName": company,
            "security_description": payload.get("security_description") or payload.get("description") or company,
            "securityDescription": payload.get("securityDescription") or payload.get("description") or company,
            "asset_class": payload.get("asset_class") or "equity",
            "assetClass": payload.get("assetClass") or "equity",
            "instrument_type": payload.get("instrument_type") or "equity",
            "instrumentType": payload.get("instrumentType") or "equity",
            "event_type": CONGRESS_EQUITY_EVENT_TYPE,
            "eventType": CONGRESS_EQUITY_EVENT_TYPE,
        }
    )
    event.payload_json = json.dumps(payload, sort_keys=True)


def _refresh_outcomes(db, events: list[Event]) -> dict[str, int]:
    if not events:
        return {"computed": 0, "inserted": 0, "updated": 0}
    rows = compute_congress_trade_outcomes(db=db, events=events, benchmark_symbol="^GSPC")
    existing = {
        row.event_id: row
        for row in db.execute(select(TradeOutcome).where(TradeOutcome.event_id.in_([event.id for event in events]))).scalars()
    }
    now = datetime.now(timezone.utc)
    inserted = updated = 0
    for payload in rows:
        event_id = payload.get("event_id")
        target = existing.get(event_id)
        if target is None:
            target = TradeOutcome(event_id=event_id)
            db.add(target)
            inserted += 1
        else:
            updated += 1
        target.member_id = payload.get("member_id")
        target.member_name = payload.get("member_name")
        target.symbol = payload.get("symbol")
        target.trade_type = payload.get("trade_type")
        target.source = payload.get("source")
        target.trade_date = _parse_date(payload.get("trade_date"))
        target.entry_price = payload.get("entry_price")
        target.entry_price_date = _parse_date(payload.get("entry_price_date"))
        target.current_price = payload.get("current_price")
        target.current_price_date = _parse_date(payload.get("current_price_date"))
        target.benchmark_symbol = payload.get("benchmark_symbol") or "^GSPC"
        target.benchmark_entry_price = payload.get("benchmark_entry_price")
        target.benchmark_current_price = payload.get("benchmark_current_price")
        target.return_pct = payload.get("return_pct")
        target.benchmark_return_pct = payload.get("benchmark_return_pct")
        target.alpha_pct = payload.get("alpha_pct")
        target.holding_days = payload.get("holding_days")
        target.amount_min = payload.get("amount_min")
        target.amount_max = payload.get("amount_max")
        target.scoring_status = payload.get("scoring_status") or "unknown"
        target.scoring_error = payload.get("scoring_error")
        target.methodology_version = payload.get("methodology_version") or "congress_v1"
        target.computed_at = now
    return {"computed": len(rows), "inserted": inserted, "updated": updated}


def run_repair(*, since_report_date: date, apply: bool, limit: int = 500) -> dict[str, Any]:
    db = SessionLocal()
    try:
        canonical_map, historical_map, reviewed_alias_map = _build_issuer_resolution_maps(db)
        events = db.execute(
            select(Event)
            .where(Event.event_type == CONGRESS_EQUITY_EVENT_TYPE)
            .order_by(Event.event_date.desc().nullslast(), Event.ts.desc(), Event.id.desc())
            .limit(limit)
        ).scalars().all()
        candidates = []
        for event in events:
            payload = _payload(event.payload_json)
            report_date = _report_date(event, payload)
            if report_date is None or report_date < since_report_date:
                continue
            if not _needs_repair(event, payload):
                continue
            candidates.append(
                _candidate_from_event(db, event, payload, canonical_map, historical_map, reviewed_alias_map)
            )

        safe = [candidate for candidate in candidates if candidate["safe_to_apply"]]
        repaired_events: list[Event] = []
        if apply:
            for candidate in safe:
                _apply_identity(candidate)
                repaired_events.append(candidate["event"])
            outcome_refresh = _refresh_outcomes(db, repaired_events)
            db.commit()
        else:
            outcome_refresh = {"computed": 0, "inserted": 0, "updated": 0}
            db.rollback()

        rows = [
            {
                key: value
                for key, value in candidate.items()
                if key not in {"event", "payload", "transaction", "filing", "member", "security"}
            }
            for candidate in candidates
        ]
        return {
            "mode": "apply" if apply else "dry-run",
            "since_report_date": since_report_date.isoformat(),
            "scanned_limit": limit,
            "candidates": len(candidates),
            "safe_to_apply": len(safe),
            "repaired": len(repaired_events) if apply else 0,
            "outcome_refresh": outcome_refresh,
            "rows": rows,
        }
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def main() -> None:
    args = _parse_args()
    result = run_repair(
        since_report_date=_parse_date(args.since_report_date),
        apply=bool(args.apply),
        limit=args.limit,
    )
    print(json.dumps(result, indent=2, sort_keys=True, default=str))


if __name__ == "__main__":
    main()
