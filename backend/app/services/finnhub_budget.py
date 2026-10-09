"""Shared, opt-in request budget and provider cooldown for all Finnhub workers."""
from datetime import datetime, timedelta, timezone
import json
import os

from sqlalchemy import text
from app.db import SessionLocal
from app.models import InsightsSnapshot

KEY = 'finnhub:request-budget:v1'
LIMIT = 45  # Reserved headroom below the advertised free account minute budget.


def enabled():
    return os.getenv('FINNHUB_SHARED_LIMITER_ENABLED', '0') == '1'


def _state(db, now):
    from app.services.finnhub_research import FinnhubUnavailable
    if db.get_bind().dialect.name == 'postgresql':
        if not db.scalar(text('SELECT pg_try_advisory_xact_lock(hashtext(:key))'), {'key': KEY}):
            raise FinnhubUnavailable('request_budget_busy')
    row = db.get(InsightsSnapshot, KEY, populate_existing=True)
    if row is None:
        row = InsightsSnapshot(kind=KEY, source='finnhub_budget', fetched_at=now, payload_json='{}')
        db.add(row)
    state = json.loads(row.payload_json)
    if not isinstance(state, dict):
        raise FinnhubUnavailable('request_budget_unavailable')
    return row, state


def reserve(*, now=None):
    if not enabled():
        return
    from app.services.finnhub_research import FinnhubUnavailable
    now = now or datetime.now(timezone.utc)
    try:
        with SessionLocal() as db:
            row, state = _state(db, now)
            until = datetime.fromisoformat(state['blocked_until']) if state.get('blocked_until') else None
            if until and now < until:
                raise FinnhubUnavailable('provider_cooldown')
            # Sliding window avoids a double burst at a minute boundary.
            recent = [value for value in state.get('reservations', []) if now.timestamp()-60 < value]
            if len(recent) >= LIMIT:
                raise FinnhubUnavailable('request_budget_exhausted')
            recent.append(now.timestamp())
            state['reservations'] = recent
            row.payload_json, row.fetched_at = json.dumps(state, sort_keys=True), now
            db.commit()
    except FinnhubUnavailable:
        raise
    except Exception:
        raise FinnhubUnavailable('request_budget_unavailable') from None


def cooldown(retry_after=None, *, now=None):
    if not enabled():
        return
    now = now or datetime.now(timezone.utc)
    try:
        seconds = max(60, min(3600, int(retry_after or 60)))
    except (TypeError, ValueError):
        seconds = 60
    from app.services.finnhub_research import FinnhubUnavailable
    try:
        with SessionLocal() as db:
            row, state = _state(db, now)
            old = datetime.fromisoformat(state['blocked_until']) if state.get('blocked_until') else now
            state['blocked_until'] = max(old, now+timedelta(seconds=seconds)).isoformat()
            row.payload_json, row.fetched_at = json.dumps(state, sort_keys=True), now
            db.commit()
    except FinnhubUnavailable:
        raise
    except Exception:
        raise FinnhubUnavailable('request_budget_unavailable') from None
