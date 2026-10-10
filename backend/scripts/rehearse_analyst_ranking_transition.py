import argparse,copy,hashlib,json,os,sys
from datetime import datetime,timezone,timedelta
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
os.environ['DATABASE_URL']='sqlite:///:memory:'
os.environ['ANALYST_PROVIDER']='finnhub'
from sqlalchemy import create_engine,select
from sqlalchemy.orm import Session
from app.db import Base
from app.models import LeaderboardSnapshot,ConfirmationMonitoringSnapshot,ConfirmationMonitoringEvent,UserAccount
from app.services import confirmation_score as score,confirmation_monitoring as monitor,top_stocks,top_ideas_digest
from app.entitlements import ENTITLEMENTS
from app.services.ranking_access import project_ranking
parser=argparse.ArgumentParser(description='Offline complete saved ranking and monitoring transition, no delivery')
parser.add_argument('--baseline',type=Path,required=True)
parser.add_argument('--samples',type=Path,required=True)
parser.add_argument('--output',type=Path,required=True)
args=parser.parse_args()
if args.output.exists():parser.error('Choose a new output receipt path')
baseline=json.loads(args.baseline.read_text(encoding='utf-8'))
sample=json.loads(args.samples.read_text(encoding='utf-8'))
assert baseline['transaction_read_only'] and sample['transaction_read_only']
original=baseline['payload'];payload=copy.deepcopy(original)
before={};after={};changes=[]
for row in payload['candidate_rows']:
    symbol=row['symbol'];old=row['confirmation_bundle'];before[symbol]=old
    computed=score.confirmation_score_bundle_from_source_payloads(symbol,lookback_days=old['lookback_days'],sources_payload=old['sources'])
    assert [computed[k] for k in ['score','band','direction']]==[old[k] for k in ['score','band','direction']],symbol
    sources=copy.deepcopy(old['sources']);sources['analysts']=score._empty_source('Analysts unavailable').as_dict()
    new=score.confirmation_score_bundle_from_source_payloads(symbol,lookback_days=old['lookback_days'],sources_payload=sources)
    new['score_context_version']=old['score_context_version']
    after[symbol]=new;row['confirmation_bundle']=new;row['confirmation']=new
    if old['score']!=new['score'] or old['direction']!=new['direction']:
        changes.append(dict(symbol=symbol,before_score=old['score'],after_score=new['score'],before_direction=old['direction'],after_direction=new['direction']))
# Independently captured live current score samples must use the identical transformation.
for symbol,old in sample['before'].items():
    sources=copy.deepcopy(old['sources']);sources['analysts']=score._empty_source('Analysts unavailable').as_dict()
    result=score.confirmation_score_bundle_from_source_payloads(symbol,lookback_days=old['lookback_days'],sources_payload=sources)
    assert all(result[k]==sample['after'][symbol][k] for k in ['score','band','direction','scoring_version'])
engine=create_engine('sqlite:///:memory:');Base.metadata.create_all(engine)
now=datetime.now(timezone.utc)
def no_io(*a,**k):raise AssertionError('Unexpected external operation')
with Session(engine) as db,patch('requests.sessions.Session.request',no_io),patch('app.services.data_enrichment_queue.enqueue_data_enrichment_job',no_io):
    leaderboard=LeaderboardSnapshot(leaderboard_key='top_stocks',generated_at=datetime.fromisoformat(baseline['generated_at']),payload_json=json.dumps(original))
    db.add(leaderboard);db.commit()
    assert top_stocks.build_top_stocks_response(db)['items']==[]
    leaderboard.payload_json=json.dumps(payload);db.commit()
    prepared=top_stocks.build_top_stocks_response(db)
    public=project_ranking(prepared,authenticated=True,entitlements=ENTITLEMENTS['premium'],stocks=True,full=True)
    assert len(public['items'])<=10
    assert prepared==top_stocks.build_top_stocks_response(db)
    # Existing historical monitoring is not a migration alert.
    history=ConfirmationMonitoringEvent(user_id=1,watchlist_id=1,ticker='MSFT',event_type='confirmation_upgraded',title='Recorded history',score_after=80,band_after='strong',direction_after='bullish',source_count_after=4,created_at=now-timedelta(days=1),payload_json='{}')
    db.add(history)
    for symbol,old in before.items():
        state=monitor.monitoring_state_from_bundle(symbol,old,observed_at=now-timedelta(minutes=1))
        db.add(monitor._snapshot_from_state(user_id=1,watchlist_id=1,state=state))
    db.commit()
    historic={c.name:str(getattr(history,c.name)) for c in history.__table__.columns}
    with patch.object(monitor,'get_confirmation_score_bundles_for_tickers',lambda *a,**k:after):
        first=monitor.refresh_watchlist_confirmation_monitoring(db,user_id=1,watchlist_id=1,tickers=list(before),now=now);db.commit()
        second=monitor.refresh_watchlist_confirmation_monitoring(db,user_id=1,watchlist_id=1,tickers=list(before),now=now);db.commit()
    assert first['initialized']==len(before) and first['generated']==0
    assert second['initialized']==second['generated']==0
    assert db.query(ConfirmationMonitoringEvent).count()==1
    assert historic=={c.name:str(getattr(history,c.name)) for c in history.__table__.columns}
    digests={}
    for frequency in ['daily','weekly']:
        user=UserAccount(email='migration@example.test',first_name='Fixture',top_stock_ideas_frequency=frequency)
        with patch.object(top_ideas_digest,'entitlements_for_user',lambda *a:ENTITLEMENTS['premium']):
            first_digest=top_ideas_digest.build_top_ideas_digest(db,user,now=now)
            second_digest=top_ideas_digest.build_top_ideas_digest(db,user,now=now)
        assert first_digest.context==second_digest.context and first_digest.items==second_digest.items
        digests[frequency]=dict(items=first_digest.items_count,symbols=[r['symbol'] for r in first_digest.items],repeat_identical=True)
    report=dict(status='passed',source_snapshot_sha256=baseline['sha256'],source_generated_at=baseline['generated_at'],candidate_count=len(before),changed_scores_or_directions=len(changes),current_sample_matches=len(sample['symbols']),monitoring_initialized=first['initialized'],new_monitoring_events=0,historical_events_unchanged=True,repeat_identical=True,top_symbols=[r['symbol'] for r in public['items']],digests=digests,changes=changes,source_requests=0,emails=0,production_writes=0,limitation='Saved public source replay; full fresh production ranking rebuild and activation are still required.')
    args.output.write_text(json.dumps(report,indent=2),encoding='utf-8')
    print(json.dumps({k:v for k,v in report.items() if k!='changes'}))
