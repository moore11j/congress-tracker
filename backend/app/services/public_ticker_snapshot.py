"""Bounded dated research for cache-only public renders between worker refreshes."""
import json
from datetime import datetime, timedelta, timezone


MAX_AGE = timedelta(hours=24)


def prepared_snapshot(row, *, symbol: str, now: datetime) -> dict | None:
    if row is None or row.symbol != symbol or row.user_segment != "canonical":
        return None
    generated = row.generated_at
    if not isinstance(generated, datetime):
        return None
    if generated.tzinfo is None:
        generated = generated.replace(tzinfo=timezone.utc)
    if not timedelta(0) <= now - generated <= MAX_AGE:
        return None
    try:
        payload = json.loads(row.payload_json)
    except (ValueError, TypeError):
        return None
    if not isinstance(payload, dict) or payload.get("symbol") != symbol:
        return None
    # The caller still validates the current score contract and applies the
    # anonymous entitlement projection. Never refresh source or score dates.
    return payload
