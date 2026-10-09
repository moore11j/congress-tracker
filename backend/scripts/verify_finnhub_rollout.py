"""Read-only rollout receipt: hashes, safe flags, cache counts and scheduled results."""
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import platform
import sys

now = datetime.now(timezone.utc)
files = ['app/services/finnhub_research.py','app/services/finnhub_budget.py',
         'app/services/replacement_news.py','app/services/replacement_news_events.py',
         'app/services/finnhub_news_warming.py','app/jobs/warm_finnhub_news.py',
         'app/services/watchlist_content_events.py','app/services/data_enrichment_queue.py',
         'app/services/insights_snapshots.py','app/services/email_digests.py',
         'app/services/operational_intelligence.py','crontab',
         'app/jobs/warm_free_research.py','app/services/free_calendar.py',
         'app/services/finnhub_free_data.py','app/services/replacement_analysts.py',
         'app/services/analyst_consensus.py','app/services/event_calendar.py',
         'app/services/confirmation_score.py','app/jobs/refresh_analyst_consensus.py',
         'app/jobs/refresh_analyst_events.py']
hashes = {name: hashlib.sha256(Path('/app',name).read_bytes()).hexdigest() if Path('/app',name).exists() else None for name in files}
flags = {name:os.getenv(name) for name in ('NEWS_PROVIDER','FINNHUB_NEWS_WARMING_ENABLED',
    'FINNHUB_SHARED_LIMITER_ENABLED','FINNHUB_NEWS_PUBLICATION_ENABLED',
    'FREE_RESEARCH_WARMING_ENABLED','ANALYST_PROVIDER','CALENDAR_PROVIDER',
    'NEWS_PUBLISH_SINCE','NEWS_PUBLISH_AFTER')}
if os.getenv('FINNHUB_RECEIPT_HASH_ONLY') == '1':
    print('FINNHUB_RECEIPT='+json.dumps({'observed_at':now.isoformat(),'runtime':platform.python_version(),
        'hashes':hashes,'flags':flags,'key_configured':bool(os.getenv('FINNHUB_API_KEY','').strip())}))
    sys.exit(0)
from sqlalchemy import select, text, func
from sqlalchemy.orm import Session
from app.db import engine
from app.models import InsightsSnapshot, Event
assert engine.dialect.name == 'postgresql'
with engine.connect() as conn, conn.begin():
    conn.execute(text('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY'))
    conn.execute(text("SET LOCAL statement_timeout = '20s'"))
    assert conn.scalar(text('SHOW transaction_read_only')) == 'on'
    with Session(bind=conn) as db:
        receipt = db.get(InsightsSnapshot,'finnhub:news-warming:v1')
        state = json.loads(receipt.payload_json) if receipt else {}
        runs = []
        for run in state.get('runs',[])[-5:]:
            runs.append({key:run.get(key) for key in ('status','observed_at','universe_size','universe_truncated',
                'planned_scopes','completed_scopes','public_selection','canonical_writes','emails',
                'fresh_company_caches','missing_company_caches','stale_company_caches','coverage_complete')})
            runs[-1]['headlines_cache'] = run.get('headlines_cache')
            runs[-1]['errors'] = [{'scope':r['scope'],'status':r['status'],'reason':r.get('reason')}
                for r in run.get('results',[]) if r.get('stale') or r['status'] not in {'ok','empty'}]
        caches = list(db.execute(select(InsightsSnapshot.kind,InsightsSnapshot.fetched_at)
            .where(InsightsSnapshot.kind.like('finnhub-news:%')).limit(2001)))
        def aware(stamp):
            return stamp.replace(tzinfo=timezone.utc) if stamp.tzinfo is None else stamp
        stale = sum(now-aware(stamp)>timedelta(minutes=30) for _,stamp in caches)
        recent_events = db.scalar(select(func.count()).select_from(Event).where(
            Event.ts>=datetime(2026,10,9,19,34,tzinfo=timezone.utc),Event.source_provider=='finnhub'))
        # Public provider material only, for an isolated exact-release replay.
        sample = list(db.scalars(select(InsightsSnapshot).where(
            InsightsSnapshot.kind.like('finnhub-news:%'), InsightsSnapshot.source == 'finnhub')
            .order_by(InsightsSnapshot.kind.desc()).limit(101)))
        public_caches = [{'kind':r.kind,'fetched_at':aware(r.fetched_at).isoformat(),
                         'payload':json.loads(r.payload_json)} for r in sample]
        symbols = [r.kind.split(':',2)[2] for r in sample if r.kind.startswith('finnhub-news:company:')]
        legacy = list(db.scalars(select(Event).where(Event.event_type=='news_article',
            Event.symbol.in_(symbols), Event.event_date>=now-timedelta(days=7))
            .order_by(Event.id).limit(3001))) if symbols else []
        # News events are public issuer material. No watchlist/user/delivery rows.
        legacy_public = [{key:(aware(value).isoformat() if isinstance(value,datetime) else value)
            for key in ('id','event_type','symbol','ts','event_date','source','source_provider',
                        'data_source','source_filing_id','source_document_url','impact_score','payload_json')
            for value in [getattr(event,key)]} for event in legacy[:3000]]
        headline_views = [{'kind':r.kind,'source':r.source,'fetched_at':aware(r.fetched_at).isoformat(),
                          'payload':json.loads(r.payload_json)} for r in db.scalars(select(InsightsSnapshot).where(
                              InsightsSnapshot.kind.in_(['market-headlines','market-headlines:finnhub'])))]
        research_row = db.get(InsightsSnapshot, 'free-research:warming:v1')
        research_state = json.loads(research_row.payload_json) if research_row else {}
        research_caches = list(db.scalars(select(InsightsSnapshot).where(
            (InsightsSnapshot.kind.like('free-calendar:%')) |
            (InsightsSnapshot.kind.like('finnhub:recommendations:%'))).order_by(InsightsSnapshot.kind).limit(101)))
        research_public = [{'kind':r.kind,'source':r.source,'fetched_at':aware(r.fetched_at).isoformat(),
                            'payload':json.loads(r.payload_json)} for r in research_caches]
        result = {'observed_at':now.isoformat(),'runtime':platform.python_version(),'hashes':hashes,
            'flags':flags,'key_configured':bool(os.getenv('FINNHUB_API_KEY','').strip()),
            'news_cache_count':len(caches),'cache_count_truncated':len(caches)>2000,
            'cache_older_than_30_minutes':stale,'recent_finnhub_events':recent_events,
            'scheduled_runs':runs,'public_news_caches':public_caches,
            'public_news_caches_truncated':len(sample)>100,
            'warming_lease_until':(state.get('lease') or {}).get('until'),
            'legacy_news_events':legacy_public,'legacy_news_truncated':len(legacy)>3000,
            'public_headline_views':headline_views,
            'free_research_runs':research_state.get('runs', [])[-5:],
            'free_research_lease_until':(research_state.get('lease') or {}).get('until'),
            'public_research_caches':research_public,'research_caches_truncated':len(research_caches)>100,
            'research_cache_count':len(research_caches),
            'transaction_read_only':True,'database_writes':0,'customer_rows_exported':0}
print('FINNHUB_RECEIPT='+json.dumps(result,sort_keys=True))
