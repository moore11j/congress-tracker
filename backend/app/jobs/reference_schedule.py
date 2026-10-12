"""Shared calendar and retry rules for institutional reference preparation."""
from datetime import date, datetime, timedelta
import re

PLAN_VERSION = 6
PLAN_TTL = timedelta(minutes=15)


def periods(today):
    year, quarter = today.year, (today.month-1)//3+1
    result = set()
    for _ in range(2):
        quarter -= 1
        if quarter == 0: year, quarter = year-1, 4
        end = date(year,12,31) if quarter == 4 else date(year,quarter*3+1,1)-timedelta(days=1)
        result.add(end.isoformat())
    return result


def attempts(attempted_at, now):
    retained, times, recent = {}, {}, {}
    eligible_periods = periods(now.date())
    for key, stamp in attempted_at.items():
        try:
            cusip, period = key.split(':')
            previous = datetime.fromisoformat(stamp)
            if (period in eligible_periods and re.fullmatch(r'[A-Z0-9]{9}', cusip)
                    and previous.tzinfo is not None):
                retained[key] = stamp
                times[key] = previous
                if now-previous < timedelta(days=1):
                    recent[key] = stamp
        except (ValueError, TypeError, AttributeError):
            continue
    return retained, times, recent


def exhausted_plan(state, now):
    """Prove zero due scopes without trusting cached success/coverage counts.

    Recent attempts alone exclude a scope in the full planner. Verified source
    records can only exclude additional scopes, so this proof does not need to
    load their payloads. Any unattempted/expired key falls through.
    """
    universe = state.get('universe')
    planned_at = state.get('universe_prepared_at')
    if (not isinstance(universe, dict) or universe.get('version') != PLAN_VERSION
            or universe.get('periods') != sorted(periods(now.date())) or not planned_at):
        return None
    stamp = datetime.fromisoformat(planned_at)
    if not timedelta(0) <= now-stamp < PLAN_TTL:
        return None
    weights = universe.get('weights')
    if not isinstance(weights, dict) or len(weights)>5000:
        return None
    _, _, recent = attempts(state.get('attempted_at', {}), now)
    if any(key not in recent for key in weights):
        return None
    # Never describe held or truncated evidence as complete preparation.
    if type(universe.get('universe_truncated')) is not bool or type(universe.get('held_documents')) is not int:
        return None
    if universe['held_documents'] < 0:
        return None
    return {'status': 'partial' if universe['universe_truncated'] or universe['held_documents'] else 'ok',
        'reason': 'cached_plan_has_no_due_scopes', 'preflight': True,
        'planned_scopes': 0, 'completed_scopes': 0, 'pending_scopes': 0,
        'universe_size': len(weights), 'universe_truncated': universe['universe_truncated'],
        'held_documents': universe['held_documents'], 'public_writes': 0, 'price_requests': 0, 'emails': 0}
