"""Bounded public disclosure rows, read only from persisted data.

No provider calls, enrichment jobs, scores, outcomes, or paid institutional data.
The same preview is served to anonymous readers and server-rendered pages.
"""
import json
from datetime import datetime, timedelta, timezone

from sqlalchemy import select, func

from app.models import Event
from app.services.event_activity_filters import insider_visibility_clause

PUBLIC_TYPES = {
    "insider": ("insider_trade",),
    "congress": ("congress_trade", "congress_treasury_trade", "congress_crypto_trade"),
}
PUBLIC_PAYLOAD_KEYS = (
    "insider_name", "reporting_cik", "role", "insiderRole", "company_name",
    "transaction_date", "filing_date", "report_date", "transaction_type",
    "security_name", "shares", "ownership", "asset_type", "asset_class",
)


def _public_text(payload, *keys):
    # Providers use different field names. Read only specified public fields;
    # never return the full raw filing/provider envelope.
    for record in (payload, payload.get("payload"), payload.get("raw")):
        if isinstance(record, dict):
            for key in keys:
                value = record.get(key)
                if isinstance(value, str) and value.strip():
                    return value.strip()
    return None


def public_activity(db, *, tape, symbol=None, recent_days=365, limit=21, offset=0, trade_type=None):
    if tape not in PUBLIC_TYPES:
        raise ValueError("Only public Congress and insider disclosures are supported")
    limit = max(1, min(int(limit), 21))
    offset = max(0, min(int(offset), 2000))
    recent_days = max(1, min(int(recent_days), 365))
    q = select(Event).where(
        Event.event_type.in_(PUBLIC_TYPES[tape]),
        Event.ts >= datetime.now(timezone.utc) - timedelta(days=recent_days),
    )
    if symbol:
        q = q.where(Event.symbol == symbol.strip().upper())
    if tape == "insider":
        q = q.where(insider_visibility_clause())
    if trade_type:
        values = ("purchase", "buy", "p-purchase") if trade_type == "purchase" else ("sale", "sell", "s-sale")
        q = q.where(func.lower(Event.trade_type).in_(values))
    rows = db.execute(q.order_by(Event.ts.desc(), Event.id.desc()).offset(offset).limit(limit + 1)).scalars().all()
    items = []
    for event in rows[:limit]:
        try:
            payload = json.loads(event.payload_json or "{}")
        except (TypeError, ValueError):
            payload = {}
        if not isinstance(payload, dict):
            payload = {}
        public_payload = {key: payload[key] for key in PUBLIC_PAYLOAD_KEYS if key in payload}
        member_name = event.member_name
        if event.event_type == "insider_trade":
            member_name = _public_text(payload, "insider_name", "insiderName", "reporting_name", "reportingName", "reporting_owner_name", "reportingOwnerName", "owner_name", "ownerName") or event.member_name
            public_payload["insider_name"] = member_name
            public_payload["role"] = _public_text(payload, "role", "relationship", "title", "typeOfOwner", "officerTitle", "insiderRole", "position")
            public_payload["reporting_cik"] = _public_text(payload, "reporting_cik", "reportingCik", "reportingCIK", "rptOwnerCik")
        public_payload["transaction_date"] = _public_text(payload, "transaction_date", "transactionDate", "trade_date", "tradeDate")
        public_payload["filing_date"] = _public_text(payload, "filing_date", "filingDate", "report_date", "reportDate")
        items.append({
            "id": event.id, "event_type": event.event_type, "ts": event.ts,
            "symbol": event.symbol, "member_name": member_name,
            "member_bioguide_id": event.member_bioguide_id,
            "chamber": event.chamber, "party": event.party,
            "trade_type": event.trade_type, "amount_min": event.amount_min,
            "amount_max": event.amount_max, "source": event.source,
            "url": event.source_document_url or _public_text(payload, "url", "source_url", "sourceUrl", "filing_url", "filingUrl", "report_url", "reportUrl", "document_url", "documentUrl", "sec_url", "secUrl", "finalLink", "link"),
            "payload": public_payload,
        })
    return {"items": items, "has_more": len(rows) > limit, "limit": limit, "offset": offset,
            "status": "ok" if items else "empty", "window_days": recent_days}
