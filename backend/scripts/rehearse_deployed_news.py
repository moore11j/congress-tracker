"""Replay public production news caches against an explicitly selected checkout."""
import argparse
from contextlib import ExitStack
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import sys
from unittest.mock import patch

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--backend', type=Path, required=True)
p.add_argument('--receipt', type=Path, required=True)
p.add_argument('--output', type=Path, required=True)
p.add_argument('--publish-since', help='Exact proposed YYYY-MM-DD publication boundary')
p.add_argument('--publish-after', help='Aware activation timestamp; exclude earlier headlines')
a = p.parse_args()
assert not a.output.exists()
sys.path[:0] = [str(a.backend.resolve()), str((a.backend/'tests').resolve())]
os.environ['DATABASE_URL'] = 'sqlite:///:memory:'
os.environ.update(NEWS_PROVIDER='finnhub', FINNHUB_NEWS_PUBLICATION_ENABLED='1',
                  FMP_PROVIDER_DISABLED='1', FINNHUB_SHARED_LIMITER_ENABLED='0')
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from app.db import Base
from app import main as api
from app.models import Event, InsightsSnapshot, Security, WatchlistItem, MonitoringAlert, EmailDelivery
from app.services import replacement_news, email_digests as digests, fmp_news, insights_snapshots, email_intraday, walnut_takes
from app.services.watchlist_content_events import sync_watchlist_content_events
from app.services.monitoring_alerts import _ensure_alert_for_event
from test_email_digests import _user, _watchlist

blocked_calls = 0
def forbidden(*args, **kwargs):
    global blocked_calls
    blocked_calls += 1
    raise AssertionError('Unexpected network, queue or email call in isolated replay')

raw = a.receipt.read_bytes()
receipt = json.loads(raw)
assert receipt['transaction_read_only'] and receipt['database_writes'] == 0
samples = receipt['public_news_caches']
assert samples
assert not receipt.get('public_news_caches_truncated')
assert not receipt.get('legacy_news_truncated')
now = datetime.now(timezone.utc)
os.environ['NEWS_PUBLISH_SINCE'] = a.publish_since or (now-timedelta(days=7)).date().isoformat()
datetime.strptime(os.environ['NEWS_PUBLISH_SINCE'], '%Y-%m-%d')
if a.publish_after:
    os.environ['NEWS_PUBLISH_AFTER'] = receipt['observed_at'] if a.publish_after == 'capture' else a.publish_after
engine = create_engine('sqlite:///:memory:')
Base.metadata.create_all(engine)
factory = sessionmaker(bind=engine)
with ExitStack() as stack, factory() as db:
    stack.enter_context(patch('requests.sessions.Session.request', forbidden))
    stack.enter_context(patch.object(digests, '_send_digest', forbidden))
    stack.enter_context(patch.object(replacement_news, 'SessionLocal', factory))
    stack.enter_context(patch.object(replacement_news, '_enqueue', forbidden))
    stack.enter_context(patch.object(fmp_news, 'get_request_context', return_value={'path':'/api/insights/news'}))
    stack.enter_context(patch.object(digests, '_upcoming_calendar_events_for_digest', return_value=([], 'Calendar separately validated', '')))
    # Exercise actual headline fallback enrichment without a model request.
    stack.enter_context(patch.object(walnut_takes, 'resolved_setting_value', return_value=None))
    symbols = []
    for sample in samples:
        db.add(InsightsSnapshot(kind=sample['kind'], source='finnhub',
            fetched_at=datetime.fromisoformat(sample['fetched_at']), payload_json=json.dumps(sample['payload'])))
        if sample['kind'].startswith('finnhub-news:company:'):
            symbols.append(sample['kind'].split(':', 2)[2])
    db.commit()
    assert symbols
    headline_baselines = {}
    for saved in receipt.get('public_headline_views',[]):
        db.add(InsightsSnapshot(kind=saved['kind'],source=saved['source'],
            fetched_at=datetime.fromisoformat(saved['fetched_at']),payload_json=json.dumps(saved['payload'])))
        headline_baselines[saved['kind']] = saved
    db.commit()
    if headline_baselines:
        os.environ['NEWS_PROVIDER']='fmp';os.environ['FMP_PROVIDER_DISABLED']='0'
        before = insights_snapshots.get_insights_headlines(db)
        assert before['source']=='fmp' and before['items']
        assert insights_snapshots.seed_finnhub_headlines(db)['status'] in {'ok','cached'}
        os.environ['NEWS_PROVIDER']='finnhub'
        assert insights_snapshots.get_insights_headlines(db)['source']=='finnhub'
        os.environ['NEWS_PROVIDER']='fmp'
        assert insights_snapshots.get_insights_headlines(db)==before
        os.environ['NEWS_PROVIDER']='finnhub';os.environ['FMP_PROVIDER_DISABLED']='1'
    categories = {}
    for category in ('general','world-indexes','us-indexes','us-sectors','us-macro','us-treasury','commodities','crypto','currencies'):
        result = api.list_insights_category_news(category, page=0, limit=20)
        assert result['source'] == 'finnhub' and result['status'] in {'ok','empty'}
        assert not result['stale']
        categories[category] = {'count':len(result['items']), 'filter':result['category_filter']}
    stocks = {}
    for symbol in symbols:
        prepared = replacement_news.prepared_news(symbol=symbol)
        assert prepared['status'] in {'ok','empty'} and not prepared['stale']
        result = api.ticker_news(symbol, page=0, limit=20)
        # The existing ticker HTTP contract normalizes empty to no_data and
        # removes internal cache diagnostics. Check freshness at its source.
        assert result['source'] == 'finnhub' and result['status'] in {'ok','no_data'}
        assert result['as_of'] == prepared['as_of'] and result['items'] == prepared['items']
        stocks[symbol] = len(result['items'])
    headlines = insights_snapshots.refresh_insights_headlines(db)
    db.commit()
    assert headlines['source'] == 'finnhub' and headlines['status'] == 'ok'
    assert all(item['walnut_take_source']=='fallback' and item['summary'] is None for item in headlines['items'])
    assert api.list_insights_news(db=db,page=0,limit=20)['source'] == 'finnhub'
    user = _user(db, 'public-cache-replay@example.test')
    watchlist = _watchlist(db, user, alert_triggers=['news'])
    for symbol in symbols:
        security = db.scalar(select(Security).where(Security.symbol == symbol))
        if security is None:
            security = Security(symbol=symbol, name=symbol, asset_class='stock'); db.add(security); db.flush()
        if not db.scalar(select(WatchlistItem.id).where(WatchlistItem.watchlist_id == watchlist.id, WatchlistItem.security_id == security.id)):
            db.add(WatchlistItem(watchlist_id=watchlist.id, security_id=security.id, target_type='ticker'))
    db.commit()
    legacy_ids = set()
    for saved in receipt.get('legacy_news_events',[]):
        fields = dict(saved)
        for key in ('ts','event_date'):
            if fields.get(key): fields[key] = datetime.fromisoformat(fields[key])
        event = Event(**fields); db.add(event);db.flush();legacy_ids.add(event.id)
        _ensure_alert_for_event(db,user_id=user.id,watchlist=watchlist,event=event)
    db.commit()
    baseline = {event.id:(event.source_filing_id,event.ts,event.event_date,event.payload_json)
        for event in db.scalars(select(Event))}
    created = sync_watchlist_content_events(db, watchlist.id); db.commit()
    events = list(db.scalars(select(Event)))
    assert created == len(events)-len(legacy_ids) and events
    identities = [(event.id,event.source_filing_id,event.event_date) for event in events]
    for event in events:
        assert bool(_ensure_alert_for_event(db,user_id=user.id,watchlist=watchlist,event=event)) == (event.id not in legacy_ids)
        assert not _ensure_alert_for_event(db,user_id=user.id,watchlist=watchlist,event=event)
        candidate = email_intraday._watchlist_candidate(db,user,watchlist,event)
        assert candidate.event_key == f'event:{event.id}'
        assert candidate.context['alert_url'] == json.loads(event.payload_json)['url']
    db.commit()
    assert sync_watchlist_content_events(db,watchlist.id) == 0
    counts = []
    for _ in range(2):
        since = now-timedelta(days=7)
        counts.append([digests.build_monitoring_digest(db,user,watchlist,since,window_end=now+timedelta(hours=1)).items_count,
            digests.build_signal_alert_digest(db,user,since,window_end=now+timedelta(hours=1)).items_count,
            digests.build_watchlist_activity_digest(db,user,watchlist,since).items_count])
    assert counts[0] == counts[1] and all(count > 0 for count in counts[0])
    assert identities == [(event.id,event.source_filing_id,event.event_date) for event in db.scalars(select(Event))]
    assert all((db.get(Event,key).source_filing_id,db.get(Event,key).ts,db.get(Event,key).event_date,
                db.get(Event,key).payload_json)==values for key,values in baseline.items())
    assert db.query(EmailDelivery).count() == 0
    assert blocked_calls == 0, f'Unexpected network/queue/email attempts: {blocked_calls}'
    report = {'status':'passed','receipt_sha256':hashlib.sha256(raw).hexdigest(),
        'backend':str(a.backend.resolve()), 'publication_since':os.environ['NEWS_PUBLISH_SINCE'],
        'publication_after':os.getenv('NEWS_PUBLISH_AFTER'), 'categories':categories,'company_news':stocks,
        'insights_headlines':len(headlines['items']), 'events':len(events),'legacy_events_preserved':len(legacy_ids),
        'headline_switch_rollback_checked':bool(headline_baselines),
        'new_events':created,'repeat_new_events':0,
        'monitoring_alerts':db.query(MonitoringAlert).count(),'repeat_digest_counts':counts,
        'intraday_previews':len(events),'http_calls':0,'emails':0,'production_writes':0,
        'exclusions':['Model enrichment','Calendar and fresh prices','Research extraction requires full text']}
    a.output.write_text(json.dumps(report,indent=2),encoding='utf-8')
    print(json.dumps(report))
engine.dispose()
