"""Prepare free calendars and recommendations without changing public selection."""
from datetime import datetime, timedelta, timezone
import json
import os
import uuid

from sqlalchemy import func, select, text
from app.db import SessionLocal
from app.models import InsightsSnapshot, Security, Watchlist, WatchlistItem
from app.services import free_calendar, replacement_analysts
from app.services.finnhub_news_warming import _owner, _lease_active, STOP_REASONS
from app.utils.symbols import normalize_symbol

KEY = 'free-research:warming:v1'


def run():
    if os.getenv('FREE_RESEARCH_WARMING_ENABLED', '0') != '1':
        return {'status': 'disabled'}
    if not os.getenv('FINNHUB_API_KEY', '').strip():
        return {'status': 'unavailable', 'reason': 'missing_api_key'}
    if os.getenv('FINNHUB_SHARED_LIMITER_ENABLED', '0') != '1':
        return {'status': 'unavailable', 'reason': 'shared_budget_required'}
    now = datetime.now(timezone.utc)
    with SessionLocal() as db:
        if db.get_bind().dialect.name == 'postgresql':
            if not db.scalar(text('SELECT pg_try_advisory_xact_lock(hashtext(:key))'), {'key': KEY}):
                return {'status': 'busy'}
        row = db.get(InsightsSnapshot, KEY, populate_existing=True)
        state = json.loads(row.payload_json) if row else {}
        owner = _owner()
        if _lease_active(state.get('lease') or {}, now, owner):
            return {'status': 'busy', 'reason': 'active_warming_lease'}
        watched = list(db.scalars(select(func.upper(Security.symbol)).select_from(WatchlistItem)
            .join(Watchlist, Watchlist.id == WatchlistItem.watchlist_id)
            .join(Security, Security.id == WatchlistItem.security_id)
            .where(Watchlist.owner_user_id.is_not(None), WatchlistItem.target_type == 'ticker')
            .distinct().order_by(func.upper(Security.symbol)).limit(1001)))
        from app.services.data_enrichment_queue import _recently_viewed_ticker_symbols, DEFAULT_PREWARM_SYMBOLS
        symbols = sorted({s for raw in [*watched[:1000], *_recently_viewed_ticker_symbols(db, limit=100),
            *DEFAULT_PREWARM_SYMBOLS] if (s := normalize_symbol(raw))})
        attempts = {key: value for key, value in state.get('attempted_at', {}).items() if key in symbols}
        symbols.sort(key=lambda s: (attempts.get(s, ''), s))
        token = uuid.uuid4().hex
        if row is None:
            row = InsightsSnapshot(kind=KEY, source='free_direct', fetched_at=now, payload_json='{}')
            db.add(row)
        row.payload_json = json.dumps({**state, 'lease': {'token': token, 'owner': owner,
            'until': (now+timedelta(minutes=20)).isoformat()}}, sort_keys=True)
        db.commit()
    # No outer transaction while the endpoint cache holds one connection and
    # the shared Finnhub limiter uses the second cron connection.
    first = now.date().replace(day=1)
    months = [first.strftime('%Y-%m'), (first.replace(day=28)+timedelta(days=4)).strftime('%Y-%m')]
    work = [('bls', months[0]), *[('earnings', month) for month in months],
            *[('recommendations', symbol) for symbol in symbols[:20]]]
    results = []
    for dataset, scope in work:
        with SessionLocal() as db:
            try:
                result = (replacement_analysts.refresh(db, scope) if dataset == 'recommendations'
                          else free_calendar.refresh(db, dataset, scope))
                db.commit()
            except Exception as exc:
                db.rollback()
                # Provider exception messages are bounded reason codes; never
                # copy arbitrary exception text that could contain credentials.
                from app.services.finnhub_research import FinnhubUnavailable
                result = {'status': 'unavailable', 'reason': str(exc) if isinstance(exc, FinnhubUnavailable)
                          else type(exc).__name__}
        results.append({'dataset': dataset, 'scope': scope, **result})
        if dataset == 'recommendations':
            attempts[scope] = now.isoformat()
        if result.get('reason') in STOP_REASONS:
            break
    receipt = {'status': 'partial' if len(watched)>1000 or any(r['status'] not in {'ok','empty','cached'} for r in results) else 'ok',
        'observed_at': now.isoformat(), 'universe_size': len(symbols), 'universe_truncated': len(watched)>1000,
        'planned_scopes': len(work), 'completed_scopes': len(results), 'results': results,
        'analyst_selection': os.getenv('ANALYST_PROVIDER', 'fmp'),
        'calendar_selection': os.getenv('CALENDAR_PROVIDER', 'fmp'), 'canonical_writes': 0, 'emails': 0}
    with SessionLocal() as db:
        row = db.get(InsightsSnapshot, KEY, populate_existing=True, with_for_update=True)
        current = json.loads(row.payload_json) if row else {}
        if (current.get('lease') or {}).get('token') != token:
            return {**receipt, 'status': 'partial', 'reason': 'warming_lease_lost'}
        row.payload_json = json.dumps({'attempted_at': attempts, 'runs': [*current.get('runs', [])[-19:], receipt]}, sort_keys=True)
        row.fetched_at = now
        db.commit()
    return receipt


if __name__ == '__main__':
    result = run()
    print(json.dumps(result, sort_keys=True))
    raise SystemExit(1 if result['status'] in {'partial', 'unavailable'} else 0)
