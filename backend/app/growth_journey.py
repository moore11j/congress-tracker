"""Consent-based acquisition cohorts; no probabilistic identity stitching."""
from collections import defaultdict
from datetime import timezone, timedelta
import re

from app.paid_analytics import metadata


def utc(value):
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value


def acquisition_journey(rows, users, invoices, start, *, truncated=False):
    sessions = defaultdict(list)
    for row in rows:
        if row.session_id_hash:
            sessions[row.session_id_hash].append(row)
    groups = {}

    def group(source):
        return groups.setdefault(source, {"source": source, "sessions": 0, "stock_sessions": 0,
            "new_accounts": 0, "stock_before_signup": 0, "saved_accounts": 0,
            "returned_accounts": 0, "checkout_accounts": 0, "paid_accounts": 0})

    candidates = defaultdict(list)
    authenticated = defaultdict(list)
    for row in rows:
        if row.user_id is not None:
            authenticated[row.user_id].append(row)
    for events in sessions.values():
        events.sort(key=lambda row: (utc(row.created_at), row.id))
        props = metadata(events[0].metadata_json).get("properties", {})
        props = props if isinstance(props, dict) else {}
        source = props.get("acquisition_source") or "unattributed"
        source = source.lower() if isinstance(source, str) else "unattributed"
        source = source if re.fullmatch(r"[a-z0-9._ -]{1,80}", source) else "unattributed"
        bucket = group(source)
        bucket["sessions"] += 1
        bucket["stock_sessions"] += any(row.path.startswith("/ticker/") for row in events)
        identities = {row.user_id for row in events if row.user_id is not None}
        if len(identities) == 1:
            candidates[next(iter(identities))].append((source, events))
    new_users = [user for user in users if user.created_at and utc(user.created_at) >= start]
    attributed = 0
    for user in new_users:
        created = utc(user.created_at)
        # Attribute only a single-account session that actually spans account creation.
        eligible = [(source, events) for source, events in candidates[user.id]
            if utc(events[0].created_at) <= created <= utc(events[-1].created_at)]
        if not eligible:
            continue
        source, entry = min(eligible, key=lambda item: utc(item[1][0].created_at))
        bucket = group(source)
        attributed += 1
        bucket["new_accounts"] += 1
        bucket["stock_before_signup"] += any(row.path.startswith("/ticker/") and utc(row.created_at) <= created for row in entry)
        # Later actions require the authenticated identity, not merely a shared browser.
        later = [row for row in authenticated[user.id] if utc(row.created_at) >= created]
        bucket["saved_accounts"] += any(row.normalized_path == "/events/ticker_added_to_watchlist" for row in later)
        bucket["returned_accounts"] += any(not row.normalized_path.startswith("/events/") and utc(row.created_at) >= created + timedelta(days=1) for row in later)
        bucket["checkout_accounts"] += any(row.normalized_path == "/events/checkout_started" for row in later)
        bucket["paid_accounts"] += any(invoice.user_id == user.id and utc(invoice.charged_at) >= created for invoice in invoices)
    return {"sources": sorted(groups.values(), key=lambda row: (-row["sessions"], row["source"])),
        "attributed_new_accounts": attributed, "unattributed_new_accounts": len(new_users) - attributed,
        "truncated": truncated, "events_examined": len(rows)}
