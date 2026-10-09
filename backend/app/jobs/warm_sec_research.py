"""Prepare SEC financial panels without changing public selection or canonical data."""
from datetime import datetime, timedelta, timezone
import json
import os
import uuid

from sqlalchemy import func, select, text
from app.db import SessionLocal
from app.models import InsightsSnapshot, Security, Watchlist, WatchlistItem
from app.services import sec_financial_statements
from app.background_job_guard import check_background_job_guard
from app.jobs.collect_direct_feeds import collector_lock
from app.clients.direct_sources import DirectSourceError
from app.services.finnhub_news_warming import _owner, _lease_active
from app.utils.symbols import normalize_symbol

KEY = 'sec-research:warming:v1'


def _run_locked():
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
    # Planning commits before nested directory/panel cache transactions.
    results = []
    for symbol in symbols[:20]:
        try:
            payload = sec_financial_statements.prepared(symbol)
            result = {'scope': symbol, 'status': payload['status'],
                'annual_periods': len(payload.get('annual', [])),
                'quarterly_periods': len(payload.get('quarterly', [])),
                'source': payload.get('source'), 'updated_at': payload.get('updatedAt')}
        except Exception as exc:
            result = {'scope': symbol, 'status': 'unavailable', 'reason': type(exc).__name__}
            if isinstance(exc, DirectSourceError):
                # This transport accepts only fixed public SEC hosts; no credentials.
                result['reason'] = str(exc)[:300]
        results.append(result)
        attempts[symbol] = now.isoformat()
        if str(result.get('reason', '')).startswith(('Source transport failed', 'Source cooldown', 'Source HTTP 403:', 'Source HTTP 429:')):
            break
    receipt = {'status': 'partial' if len(watched)>1000 or any(r['status'] not in {'ok','partial'} for r in results) else 'ok',
        'observed_at': now.isoformat(), 'universe_size': len(symbols), 'universe_truncated': len(watched)>1000,
        'planned_scopes': min(20, len(symbols)), 'completed_scopes': len(results), 'results': results,
        'financial_selection': os.getenv('FINANCIAL_STATEMENTS_PROVIDER', 'fmp'),
        'metadata_selection': os.getenv('COMPANY_METADATA_PROVIDER', 'fmp'), 'canonical_writes': 0, 'emails': 0}
    with SessionLocal() as db:
        row = db.get(InsightsSnapshot, KEY, populate_existing=True, with_for_update=True)
        current = json.loads(row.payload_json) if row else {}
        if (current.get('lease') or {}).get('token') != token:
            return {**receipt, 'status': 'partial', 'reason': 'warming_lease_lost'}
        row.payload_json = json.dumps({'attempted_at': attempts, 'runs': [*current.get('runs', [])[-19:], receipt]}, sort_keys=True)
        row.fetched_at = now
        db.commit()
    return receipt


def run():
    if os.getenv('SEC_RESEARCH_WARMING_ENABLED', '0') != '1':
        return {'status': 'disabled'}
    guard = check_background_job_guard('sec-research-warming')
    if not guard.proceed:
        return {'status': 'held', **guard.to_dict()}
    # Share SEC collector serialization; detach its lock connection from pool.
    with collector_lock() as acquired:
        if not acquired:
            return {'status': 'busy', 'reason': 'source_collection_active'}
        return _run_locked()


if __name__ == '__main__':
    result = run()
    print(json.dumps(result, sort_keys=True))
    raise SystemExit(1 if result['status'] in {'partial', 'unavailable'} else 0)
