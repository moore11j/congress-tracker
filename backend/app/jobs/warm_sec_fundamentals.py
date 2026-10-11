"""Prepare SEC financial ratios without changing public selection or canonical data."""
from datetime import datetime, timedelta, timezone
import json
import os
import time
import uuid

from sqlalchemy import func, select, text
from app.db import SessionLocal
from app.models import FundamentalsCache, InsightsSnapshot, ResearchThesis, Security, Watchlist, WatchlistItem
from app.services import sec_fundamentals_preparation
from app.background_job_guard import check_background_job_guard
from app.jobs.collect_direct_feeds import collector_lock
from app.clients.direct_sources import DirectSourceError
from app.services.finnhub_news_warming import _owner, _lease_active
from app.utils.symbols import normalize_symbol, classify_symbol

KEY = 'sec-fundamentals:warming:v1'
METRIC_MAX_SECONDS = 20


def metric_batch_limit():
    try:
        return max(1, min(8, int(os.getenv('FINNHUB_METRICS_BATCH_SIZE', '2'))))
    except ValueError:
        return 2


def metric_scopes(symbols, priority_symbols, attempts, now, limit):
    def due(symbol):
        try:
            return not timedelta(0) <= now-datetime.fromisoformat(attempts[symbol]) < timedelta(hours=24)
        except (KeyError, ValueError, TypeError):
            return True
    return sorted((s for s in symbols if due(s)),
                  key=lambda s: (s not in priority_symbols, attempts.get(s, ''), s))[:limit]


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
        research = list(db.scalars(select(func.upper(Security.symbol)).join(ResearchThesis, ResearchThesis.security_id == Security.id).where(ResearchThesis.status == 'active').distinct().order_by(func.upper(Security.symbol)).limit(1001)))
        from app.services.data_enrichment_queue import _recently_viewed_ticker_symbols, DEFAULT_PREWARM_SYMBOLS
        symbols = sorted({s for raw in [*watched[:1000], *research[:1000], *_recently_viewed_ticker_symbols(db, limit=100),
            *DEFAULT_PREWARM_SYMBOLS] if (s := normalize_symbol(raw))})
        priority_symbols = set(symbols)
        ranked = []
        broad = os.getenv('SEC_FUNDAMENTALS_UNIVERSE_WARMING_ENABLED', '0') == '1'
        if broad:
            # Retained legacy cache defines migration scope, not new FMP requests.
            ranked = list(db.scalars(select(FundamentalsCache.symbol).where(
                FundamentalsCache.provider == 'fmp', FundamentalsCache.status == 'ok')
                .distinct().order_by(FundamentalsCache.symbol).limit(5001)))
            symbols = sorted(set(symbols) | {symbol for raw in ranked[:5000]
                if (symbol := normalize_symbol(raw)) and classify_symbol(symbol)[0] == 'eligible'})
        attempts = {key: value for key, value in state.get('attempted_at', {}).items() if key in symbols}
        metric_attempts = {key: value for key, value in state.get('metric_attempted_at', {}).items() if key in symbols}
        versions = {key: value for key, value in state.get('attempted_versions', {}).items() if key in symbols}
        def due_priority(symbol):
            if not broad or symbol not in priority_symbols:
                return False
            if versions.get(symbol) != sec_fundamentals_preparation.version_for_symbol(symbol):
                return True
            try:
                last = datetime.fromisoformat(attempts[symbol])
                return not timedelta(0) <= now-last < timedelta(hours=20)
            except (KeyError, ValueError, TypeError):
                return True
        symbols.sort(key=lambda s: (0 if due_priority(s) else 1, attempts.get(s, ''), s))
        batch_limit = 5
        if broad:
            batch_limit = max(1, min(10, int(os.getenv('SEC_FUNDAMENTALS_WARMING_BATCH_SIZE', '5'))))
        truncated = len(watched)>1000 or len(research)>1000 or len(ranked)>5000
        token = uuid.uuid4().hex
        if row is None:
            row = InsightsSnapshot(kind=KEY, source='free_direct', fetched_at=now, payload_json='{}')
            db.add(row)
        row.payload_json = json.dumps({**state, 'lease': {'token': token, 'owner': owner,
            'until': (now+timedelta(minutes=20)).isoformat()}}, sort_keys=True)
        db.commit()
    # Planning commits before nested directory/panel cache transactions.
    results = []
    for symbol in symbols[:batch_limit]:
        try:
            payload = sec_fundamentals_preparation.prepare(symbol)
            result = {'scope': symbol, 'status': payload['status'], 'source': payload['source'],
                      'metrics': len(payload['values']), 'reason': payload.get('reason'),
                      'observed_at': payload['observed_at']}
        except Exception as exc:
            result = {'scope': symbol, 'status': 'unavailable', 'reason': type(exc).__name__}
            if isinstance(exc, DirectSourceError):
                # This transport accepts only fixed public SEC hosts; no credentials.
                result['reason'] = str(exc)[:300]
        results.append(result)
        attempts[symbol] = now.isoformat()
        versions[symbol] = sec_fundamentals_preparation.version_for_symbol(symbol)
        if str(result.get('reason', '')).startswith(('Source transport failed', 'Source cooldown', 'Source HTTP ')):
            break
    metric_results = []
    metric_limit = metric_batch_limit()
    metric_started = time.monotonic()
    metric_stop = None
    metric_planned = 0
    if os.getenv('FINNHUB_METRICS_WARMING_ENABLED', '0') == '1':
        from app.services import finnhub_metrics
        from app.services.finnhub_research import FinnhubUnavailable
        from app.services.finnhub_news_warming import STOP_REASONS
        scopes = metric_scopes(symbols, priority_symbols, metric_attempts, now, metric_limit)
        metric_planned = len(scopes)
        for symbol in scopes:
            if time.monotonic()-metric_started >= METRIC_MAX_SECONDS:
                metric_stop = 'elapsed_budget'
                break
            attempted_at = datetime.now(timezone.utc).isoformat()
            try:
                panel = finnhub_metrics.prepare(symbol)
                # A cached response is not a new observation. Retain its actual
                # timestamp so a near-expiry hit cannot delay refresh a day.
                attempted_at = panel.get('observed_at', attempted_at)
                item = {'scope': symbol, 'status': panel['status'], 'metrics': len(panel['values']),
                        'observed_at': panel.get('observed_at')}
            except FinnhubUnavailable as exc:
                item = {'scope': symbol, 'status': 'unavailable', 'reason': str(exc)}
            except Exception as exc:
                item = {'scope': symbol, 'status': 'unavailable', 'reason': type(exc).__name__}
            metric_results.append(item)
            if item.get('reason') in STOP_REASONS or item.get('reason') == 'shared_budget_required':
                metric_stop = item.get('reason')
                break
            metric_attempts[symbol] = attempted_at
    receipt = {'status': 'partial' if truncated or metric_stop or any(r['status'] not in {'ok','partial'} for r in [*results, *metric_results]) else 'ok',
        'observed_at': now.isoformat(), 'universe_size': len(symbols), 'universe_truncated': truncated, 'ranking_universe_enabled': broad, 'priority_universe_size': len(priority_symbols), 'ranking_cache_symbols': min(len(ranked),5000), 'batch_limit': batch_limit,
        'planned_scopes': min(batch_limit, len(symbols)), 'completed_scopes': len(results), 'results': results,
        'metric_results': metric_results,
        'planned_metric_scopes': metric_planned, 'completed_metric_scopes': len(metric_results),
        'metric_batch_limit': metric_limit, 'metric_elapsed_seconds': round(time.monotonic()-metric_started, 3),
        'metric_stop_reason': metric_stop,
        'fundamentals_selection': os.getenv('FUNDAMENTALS_PROVIDER', 'fmp'),
        'canonical_writes': 0, 'model_calls': 0, 'emails': 0}
    with SessionLocal() as db:
        row = db.get(InsightsSnapshot, KEY, populate_existing=True, with_for_update=True)
        current = json.loads(row.payload_json) if row else {}
        if (current.get('lease') or {}).get('token') != token:
            return {**receipt, 'status': 'partial', 'reason': 'warming_lease_lost'}
        row.payload_json = json.dumps({'attempted_at': attempts, 'attempted_versions': versions, 'metric_attempted_at': metric_attempts, 'runs': [*current.get('runs', [])[-19:], receipt]}, sort_keys=True)
        row.fetched_at = now
        db.commit()
    return receipt


def run():
    if os.getenv('SEC_FUNDAMENTALS_WARMING_ENABLED', '0') != '1':
        return {'status': 'disabled'}
    guard = check_background_job_guard('sec-fundamentals-warming')
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
