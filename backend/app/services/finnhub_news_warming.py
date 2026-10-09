"""Bounded prepared-news warming and receipts, independently of public selection."""
from datetime import datetime, timezone
import json
import os

from sqlalchemy import func, select, text
from app.models import InsightsSnapshot, Security, Watchlist, WatchlistItem
from app.services.replacement_news import prepared_news
from app.utils.symbols import normalize_symbol

KEY = 'finnhub:news-warming:v1'
STOP_REASONS = {'rate_limited', 'provider_cooldown', 'request_budget_exhausted',
                'request_budget_busy', 'request_budget_unavailable', 'authentication_failed', 'access_denied'}


def run(db, *, limit=20):
    if os.getenv('FINNHUB_NEWS_WARMING_ENABLED', '0') != '1':
        return {'status': 'disabled'}
    if not os.getenv('FINNHUB_API_KEY', '').strip():
        return {'status': 'unavailable', 'reason': 'missing_api_key'}
    if os.getenv('FINNHUB_SHARED_LIMITER_ENABLED', '0') != '1':
        return {'status': 'unavailable', 'reason': 'shared_budget_required'}
    if db.get_bind().dialect.name == 'postgresql':
        if not db.scalar(text('SELECT pg_try_advisory_xact_lock(hashtext(:key))'), {'key': KEY}):
            return {'status': 'busy'}
    now = datetime.now(timezone.utc)
    row = db.get(InsightsSnapshot, KEY, populate_existing=True)
    state = json.loads(row.payload_json) if row else {}
    attempts = state.get('attempted_at', {})
    # Read only public ticker identities from owned watchlists. Keep an explicit
    # overflow receipt rather than claiming coverage of an unbounded universe.
    watched = list(db.scalars(select(func.upper(Security.symbol)).select_from(WatchlistItem)
        .join(Watchlist, Watchlist.id == WatchlistItem.watchlist_id)
        .join(Security, Security.id == WatchlistItem.security_id)
        .where(Watchlist.owner_user_id.is_not(None), WatchlistItem.target_type == 'ticker')
        .distinct().order_by(func.upper(Security.symbol)).limit(1001)))
    from app.services.data_enrichment_queue import _recently_viewed_ticker_symbols, DEFAULT_PREWARM_SYMBOLS
    recent = _recently_viewed_ticker_symbols(db, limit=100)
    symbols = sorted({symbol for value in [*watched[:1000], *recent, *DEFAULT_PREWARM_SYMBOLS]
                      if (symbol := normalize_symbol(value))})
    known = {f'company:{symbol}' for symbol in symbols} | {'market:general', 'market:crypto', 'market:forex'}
    attempts = {key: value for key, value in attempts.items() if key in known}
    # Failed or empty symbols must not monopolize the next run.
    symbols.sort(key=lambda symbol: (attempts.get(f'company:{symbol}', ''), symbol))
    work = [('market:general', None, 'general'), ('market:crypto', None, 'crypto'), ('market:forex', None, 'currencies')]
    work += [(f'company:{symbol}', symbol, 'general') for symbol in symbols[:max(1,min(20,limit))]]
    results = []
    for scope, symbol, category in work:
        result = prepared_news(symbol=symbol, category=category, public=False, enqueue_on_miss=False)
        attempts[scope] = now.isoformat()
        result = {key: result.get(key) for key in ('status','reason','stale','item_count','as_of')}
        results.append({'scope':scope, **result})
        if result.get('reason') in STOP_REASONS:
            break
    partial = len(watched) > 1000 or any(r.get('stale') or r['status'] not in {'ok','empty'} for r in results)
    receipt = {'status':'partial' if partial else 'ok', 'observed_at':now.isoformat(),
        'universe_size':len(symbols), 'universe_truncated':len(watched)>1000,
        'planned_scopes':len(work), 'completed_scopes':len(results), 'results':results,
        'public_selection':os.getenv('NEWS_PROVIDER','fmp'), 'canonical_writes':0, 'emails':0}
    history = [*state.get('runs',[])[-19:],receipt]
    if row is None:
        row = InsightsSnapshot(kind=KEY,source='finnhub',fetched_at=now,payload_json='{}')
        db.add(row)
    row.source, row.fetched_at = 'finnhub', now
    row.payload_json = json.dumps({'attempted_at':attempts,'runs':history},sort_keys=True)
    db.flush()
    return receipt
