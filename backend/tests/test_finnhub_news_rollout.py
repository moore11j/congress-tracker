from datetime import datetime, timedelta, timezone
import json
import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from app.db import Base
from app.models import InsightsSnapshot, DataEnrichmentJob
from app.services import finnhub_news_warming as warmer, data_enrichment_queue as queue


@pytest.fixture
def sessions(monkeypatch):
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine, autoflush=False)
    monkeypatch.setattr(queue,'SessionLocal',factory)
    monkeypatch.setenv('FINNHUB_NEWS_WARMING_ENABLED','1')
    monkeypatch.setenv('FINNHUB_API_KEY','test-key-only')
    monkeypatch.setenv('FINNHUB_SHARED_LIMITER_ENABLED','1')
    monkeypatch.setenv('NEWS_PROVIDER','fmp')
    monkeypatch.setattr(queue,'DEFAULT_PREWARM_SYMBOLS',('AAA','BBB','CCC','DDD'))
    monkeypatch.setattr(queue,'_recently_viewed_ticker_symbols',lambda *a,**kw: [])
    yield factory
    engine.dispose()


def test_warming_rotates_failed_symbols_without_selecting_public_feed(sessions,monkeypatch):
    calls=[]
    def fetch(**kw):
        calls.append(kw.get('symbol'))
        return {'source':'finnhub','status':'unavailable','reason':'provider_timeout'} if kw.get('symbol')=='AAA' else {'source':'finnhub','status':'ok','stale':False,'item_count':1}
    monkeypatch.setattr(warmer,'prepared_news',fetch)
    with sessions() as db:
        first=warmer.run(db,limit=2);db.commit()
        second=warmer.run(db,limit=2);db.commit()
        assert first['status']=='partial' and second['status']=='ok'
        assert [s for s in calls if s] == ['AAA','BBB','CCC','DDD']
        assert first['public_selection']==second['public_selection']=='fmp'
        receipt=json.loads(db.get(InsightsSnapshot,warmer.KEY).payload_json)
        assert len(receipt['runs'])==2 and second['canonical_writes']==second['emails']==0


def test_warming_requires_shared_budget_and_stops_on_provider_cooldown(sessions,monkeypatch):
    calls=[]
    monkeypatch.setattr(warmer,'prepared_news',lambda **kw: calls.append(kw) or {'status':'unavailable','reason':'provider_cooldown'})
    with sessions() as db:
        monkeypatch.setenv('FINNHUB_SHARED_LIMITER_ENABLED','0')
        assert warmer.run(db)['reason']=='shared_budget_required' and not calls
        monkeypatch.setenv('FINNHUB_SHARED_LIMITER_ENABLED','1')
        receipt=warmer.run(db)
        assert receipt['status']=='partial' and receipt['completed_scopes']==1 and len(calls)==1


def test_actual_queue_retries_rate_limited_news_then_finishes_after_recovery(sessions,monkeypatch):
    from app.services import fmp_news
    monkeypatch.setenv('NEWS_PROVIDER','finnhub')
    monkeypatch.setenv('ENRICHMENT_QUEUE_ENABLED','true')
    monkeypatch.setenv('FMP_BACKGROUND_REFRESH_ENABLED','true')
    monkeypatch.setenv('DATA_ENRICHMENT_QUEUE_PER_JOB_GUARD_ENABLED','false')
    outcomes=iter([{'source':'finnhub','status':'unavailable','reason':'rate_limited'},
                   {'source':'finnhub','status':'empty','stale':False}])
    monkeypatch.setattr(fmp_news,'get_stock_news',lambda **kw: next(outcomes))
    assert queue.enqueue_data_enrichment_job(job_type='news_stock',symbol='AAA')
    queue.process_data_enrichment_jobs(limit=1)
    with sessions() as db:
        job=db.scalar(select(DataEnrichmentJob))
        assert job.status=='queued' and job.attempts==1 and job.reason=='rate_limited'
        assert job.next_run_at > datetime.now(timezone.utc).replace(tzinfo=None)
        job.next_run_at=datetime.now(timezone.utc)-timedelta(seconds=1);db.commit()
    queue.process_data_enrichment_jobs(limit=1)
    with sessions() as db:
        assert db.scalar(select(DataEnrichmentJob.status))=='done'


def test_stale_finnhub_cache_cannot_mark_refresh_job_successful():
    with pytest.raises(queue.ReplacementRefreshUnavailable,match='replacement_cache_stale'):
        queue._raise_for_retryable_provider_result({'source':'finnhub','status':'ok','stale':True})


def test_news_activation_and_rollback_preserve_shared_event_identity(sessions,monkeypatch):
    from app.models import Event, TickerContentCache, Security, WatchlistItem
    from app.services.finnhub_research import normalize_news
    from app.services.watchlist_content_events import sync_watchlist_content_events
    from test_email_digests import _user, _watchlist
    now=datetime.now(timezone.utc)
    monkeypatch.setenv('FINNHUB_NEWS_PUBLICATION_ENABLED','1')
    monkeypatch.setenv('NEWS_PUBLISH_SINCE',(now-timedelta(days=1)).date().isoformat())
    monkeypatch.setenv('FMP_PROVIDER_DISABLED','0')
    with sessions() as db:
        user=_user(db,'rollout@example.test'); watchlist=_watchlist(db,user,alert_triggers=['news'])
        payload=normalize_news([{'id':i,'headline':f'Company update {i}','url':f'https://example.test/{i}',
            'source':'Publisher','datetime':int(now.timestamp())-60,'related':'NVDA'} for i in (1,2)],observed_at=now,symbol='NVDA')
        legacy=TickerContentCache(content_type='news',symbol='NVDA',cache_key='legacy',source='fmp',status='ok',
            fetched_at=now,item_count=1,payload_json=json.dumps({'items':payload['items'][:1]}))
        db.add(legacy);db.commit()
        assert sync_watchlist_content_events(db,watchlist.id)==1
        db.commit()
        old=db.scalar(select(Event)); preserved=(old.id,old.source_filing_id,old.ts)
        db.add(InsightsSnapshot(kind='finnhub-news:company:NVDA',source='finnhub',fetched_at=now,payload_json=json.dumps(payload)));db.commit()
        monkeypatch.setenv('NEWS_PROVIDER','finnhub')
        assert sync_watchlist_content_events(db,watchlist.id)==1
        db.commit()
        assert sync_watchlist_content_events(db,watchlist.id)==0
        db.commit()
        # FMP may return both articles on rollback, with different tracking URLs.
        legacy.payload_json=json.dumps({'items':[{**item,'url':item['url']+'?utm_source=fmp'} for item in payload['items']]});db.commit()
        monkeypatch.setenv('NEWS_PROVIDER','fmp')
        assert sync_watchlist_content_events(db,watchlist.id)==0
        assert db.query(Event).count()==2
        old=db.get(Event,preserved[0])
        assert (old.id,old.source_filing_id,old.ts)==preserved
