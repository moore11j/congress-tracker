"""Bounded prepared-news warming and receipts, independently of public selection."""
from datetime import datetime, timedelta, timezone
import json
import os
import uuid

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
    lease = state.get('lease') or {}
    if lease.get('until') and datetime.fromisoformat(lease['until']) > now:
        return {'status': 'busy', 'reason': 'active_warming_lease'}
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
    # Cron has a two-connection pool. Release this outer transaction before
    # prepared_news holds one connection and the shared budget uses the other.
    # A durable lease retains single-worker ownership across that boundary.
    token = uuid.uuid4().hex
    if row is None:
        row = InsightsSnapshot(kind=KEY,source='finnhub',fetched_at=now,payload_json='{}')
        db.add(row)
    row.source, row.fetched_at = 'finnhub', now
    row.payload_json = json.dumps({**state, 'lease':{'token':token,
        'until':(now+timedelta(minutes=20)).isoformat()}},sort_keys=True)
    db.commit()
    results = []
    for scope, symbol, category in work:
        # Refresh each selected company in its turn. Counting a still-fresh
        # cache hit as an attempt would postpone it for another whole rotation.
        result = prepared_news(symbol=symbol, category=category, public=False,
                               force_refresh=symbol is not None, enqueue_on_miss=False)
        attempts[scope] = now.isoformat()
        result = {key: result.get(key) for key in ('status','reason','stale','item_count','as_of')}
        results.append({'scope':scope, **result})
        if result.get('reason') in STOP_REASONS:
            break
    partial = len(watched) > 1000 or any(r.get('stale') or r['status'] not in {'ok','empty'} for r in results)
    from app.services.insights_snapshots import seed_finnhub_headlines
    headlines = seed_finnhub_headlines(db)
    receipt = {'status':'partial' if partial else 'ok', 'observed_at':now.isoformat(),
        'universe_size':len(symbols), 'universe_truncated':len(watched)>1000,
        'planned_scopes':len(work), 'completed_scopes':len(results), 'results':results,
        'public_selection':os.getenv('NEWS_PROVIDER','fmp'), 'canonical_writes':0, 'emails':0,
        'headlines_cache':headlines}
    row = db.get(InsightsSnapshot, KEY, populate_existing=True, with_for_update=True)
    current = json.loads(row.payload_json) if row else {}
    if (current.get('lease') or {}).get('token') != token:
        return {**receipt, 'status':'partial', 'reason':'warming_lease_lost'}
    finished = datetime.now(timezone.utc)
    stamps = dict(db.execute(select(InsightsSnapshot.kind, InsightsSnapshot.fetched_at).where(
        InsightsSnapshot.kind.in_([f'finnhub-news:company:{symbol}' for symbol in symbols]),
        InsightsSnapshot.source == 'finnhub')).all())
    def fresh(stamp):
        if stamp.tzinfo is None:
            stamp = stamp.replace(tzinfo=timezone.utc)
        return timedelta(0) <= finished-stamp <= timedelta(minutes=15)
    receipt['fresh_company_caches'] = sum(fresh(stamp) for stamp in stamps.values())
    receipt['missing_company_caches'] = len(symbols)-len(stamps)
    receipt['stale_company_caches'] = len(stamps)-receipt['fresh_company_caches']
    receipt['coverage_complete'] = receipt['fresh_company_caches'] == len(symbols) and not receipt['universe_truncated']
    history = [*current.get('runs',[])[-19:],receipt]
    row.source, row.fetched_at = 'finnhub', now
    row.payload_json = json.dumps({'attempted_at':attempts,'runs':history},sort_keys=True)
    db.flush()
    return receipt
