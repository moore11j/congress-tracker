from datetime import date, datetime, timedelta, timezone
from contextlib import contextmanager
import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session
from app.db import Base
from app.models import InsightsSnapshot
from app.clients.direct_sources import DirectSourceError
from app.services import direct_feed_collection as collection
from app.services.direct_feed_store import DirectFeedDocument, DirectFeedRun, dumps
from app.services.feed_source_control import FeedSourceControl
from app.services.senate_discovery_recovery import KEY, SOURCE, plan_window, finish_window

NOW = datetime(2026, 10, 11, 20, tzinfo=timezone.utc)

@pytest.fixture
def db():
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        yield session
    engine.dispose()

def receipt(p):
    return {**p, 'complete': True, 'source': SOURCE, 'reported_total': 0,
            'discovered': 0, 'page_sha256': ['a'*64]}

def plan(db, day=NOW.date()):
    return plan_window(db, start=day-timedelta(days=7), end=day, now=NOW)

def finish(db, p, errors=None, **changes):
    result=finish_window(db, plan=p, receipt={**receipt(p), **changes}, errors=errors or [], run_id=1, now=NOW)
    db.commit()
    return result

def test_selected_feed_recovers_from_publication_boundary(db):
    db.add(FeedSourceControl(feed='senate_ptr', provider='official_senate', generation=2, publish_since=date(2026,10,1), reason='test', updated_at=NOW))
    db.commit()
    p=plan(db)
    assert p['start']=='2026-10-01' and p['end']=='2026-10-11'
    assert finish(db,p)['caught_up']
    assert plan(db,date(2026,10,12))['start']=='2026-10-05'

def test_outage_longer_than_lookback_never_skips_dates(db):
    finish(db,plan(db))
    for day in [date(2026,10,21),date(2026,11,1)]:
        p=plan(db,day)
        assert p['start']=='2026-10-11'
        result=finish(db,p,errors=[{'feed':'senate_ptr'}])
        assert result['covered_through']=='2026-10-11' and not result['caught_up']
    assert finish(db,plan(db,date(2026,11,2)))['caught_up']

def test_anchor_survives_interruption_and_months_in_bounded_windows(db):
    p=plan(db); db.expire_all()
    expected=date.fromisoformat(p['start']); steps=0
    while True:
        p=plan(db,date(2027,5,1))
        assert date.fromisoformat(p['start'])==expected
        assert (date.fromisoformat(p['end'])-expected).days<=93
        result=finish(db,p); steps+=1
        if result['caught_up']: break
        expected=date.fromisoformat(p['end'])
    assert steps==3

@pytest.mark.parametrize('changes', [{'complete':False},{'end':'2026-10-10'},{'start':'2026-10-05'},
    {'discovered':2},{'reported_total':True},{'discovered':False},{'page_sha256':[]},{'source':'https://example.com'}])
def test_bad_receipts_do_not_advance(db,changes):
    result=finish(db,plan(db),**changes)
    assert not result['advanced'] and result['covered_through'] is None

def test_only_senate_staging_error_prevents_advance(db):
    p=plan(db)
    assert not finish(db,p,errors=[{'feed':'senate_ptr'}])['advanced']
    assert finish(db,p,errors=[{'feed':'house_ptr'}])['advanced']

def test_stale_completion_cannot_overwrite_cursor(db):
    p=plan(db); finish(db,p)
    with pytest.raises(ValueError,match='changed during'): finish(db,p)

def test_corrupt_dates_fail_closed(db):
    plan(db)
    db.get(InsightsSnapshot,KEY).payload_json=dumps({'version':1,'anchor':'2026-10-04','covered_through':'2030-01-01'})
    db.commit()
    with pytest.raises(ValueError,match='date order'): plan(db)

@pytest.mark.parametrize('message',['Source HTTP 403: denied','Source HTTP 429: limited','Source transport failed: offline'])
def test_denial_preserves_original_and_uncovered_dates(db,monkeypatch,message):
    monkeypatch.setattr(collection,'utcnow',lambda:NOW)
    saved=DirectFeedDocument(feed='senate_ptr',source_key='saved',source_url='https://efdsearch.senate.gov/saved',
        metadata_json='{}',status='parsed',checked_at=NOW-timedelta(days=30),content_hash='original')
    db.add(saved); db.commit()
    class Senate:
        search_receipt=None
        def discover(self,**kwargs):
            self.window=kwargs
            raise DirectSourceError(message)
        def get(self,url): pytest.fail('Repeated denied source request')
    senate=Senate()
    kwargs=dict(sources=['senate_ptr'],start=date(2026,10,4),end=date(2026,10,11),senate_client=senate,recover_senate_window=True)
    first=collection.collect_direct_feeds(db,object(),**kwargs)
    kwargs.update(start=date(2026,10,24),end=date(2026,10,31))
    second=collection.collect_direct_feeds(db,object(),**kwargs)
    assert senate.window=={'start':date(2026,10,4),'end':date(2026,10,31)}
    assert first['errors'] and second['errors'] and not second['senate_recovery']['advanced']
    assert saved.status=='parsed' and saved.content_hash=='original' and saved.attempts==0
    assert len(list(db.scalars(select(DirectFeedRun))))==2

def test_empty_complete_search_catches_up_without_public_writes(db,monkeypatch):
    monkeypatch.setattr(collection,'utcnow',lambda:NOW); plan(db)
    class Senate:
        search_receipt=None
        def discover(self,*,start,end):
            self.search_receipt=receipt({'start':start.isoformat(),'end':end.isoformat()})
            return []
    result=collection.collect_direct_feeds(db,object(),sources=['senate_ptr'],start=date(2026,10,24),end=date(2026,10,31),
        senate_client=Senate(),recover_senate_window=True)
    assert result['senate_recovery']['start']=='2026-10-04'
    assert result['senate_recovery']['covered_through']=='2026-10-31'
    assert result['public_writes']==0 and result['processed']==0

@pytest.mark.parametrize('extra,expected',[([],True),(['--start','2026-10-01'],False),(['--end','2026-10-10'],False)])
def test_explicit_operator_dates_remain_exact(monkeypatch,db,extra,expected):
    from app.jobs import collect_direct_feeds as job
    monkeypatch.setenv('DIRECT_FEEDS_MODE','shadow')
    monkeypatch.setattr(job,'check_background_job_guard',lambda *a:type('Guard',(),{'proceed':True})())
    @contextmanager
    def locked(): yield True
    @contextmanager
    def session(): yield db
    monkeypatch.setattr(job,'collector_lock',locked)
    monkeypatch.setattr(job,'SessionLocal',session)
    monkeypatch.setattr(job,'ensure_direct_feed_schema',lambda *a:None)
    monkeypatch.setattr(job,'DirectSourceClient',lambda **k:object())
    calls=[]
    monkeypatch.setattr(job,'collect_direct_feeds',lambda *a,**k:calls.append(k) or {'errors':[]})
    job.main(['--sources','senate_ptr',*extra])
    assert calls[0]['recover_senate_window'] is expected
    if '--start' in extra: assert calls[0]['start']==date(2026,10,1)
    if '--end' in extra: assert calls[0]['end']==date(2026,10,10)


def test_actual_original_recovery_repeat_has_one_document_and_revision(db,monkeypatch):
    from test_senate_efd import parse_search_row, row, START, END, SENATE_REAL_REPORT
    from app.services.direct_feed_store import DirectFeedRevision
    monkeypatch.setattr(collection,'utcnow',lambda:NOW)
    metadata=parse_search_row(row(),start=START,end=END)
    class Senate:
        search_receipt=None
        def discover(self,*,start,end):
            self.search_receipt={**receipt({'start':start.isoformat(),'end':end.isoformat()}),'reported_total':1,'discovered':1}
            return [metadata]
        def get(self,url): return SENATE_REAL_REPORT.read_bytes()
    kwargs=dict(sources=['senate_ptr'],start=START,end=NOW.date(),senate_client=Senate(),recover_senate_window=True)
    first=collection.collect_direct_feeds(db,object(),**kwargs)
    second=collection.collect_direct_feeds(db,object(),**kwargs)
    assert not first['errors'] and not second['errors']
    assert first['senate_recovery']['caught_up'] and second['senate_recovery']['caught_up']
    assert len(list(db.scalars(select(DirectFeedDocument))))==1
    assert len(list(db.scalars(select(DirectFeedRevision))))==1
    assert second['processed']==0 and first['public_writes']==second['public_writes']==0


def test_search_success_with_staging_failure_does_not_skip_filings(db,monkeypatch):
    monkeypatch.setattr(collection,'utcnow',lambda:NOW)
    class Senate:
        search_receipt=None
        def discover(self,*,start,end):
            self.search_receipt={**receipt({'start':start.isoformat(),'end':end.isoformat()}),'reported_total':1,'discovered':1}
            return [{'missing':'identity'}]
    result=collection.collect_direct_feeds(db,object(),sources=['senate_ptr'],start=date(2026,10,4),end=NOW.date(),
        senate_client=Senate(),recover_senate_window=True)
    assert result['senate_discovery']['complete']
    assert result['errors'] and not result['senate_recovery']['advanced']
    assert plan(db,date(2026,11,1))['start']=='2026-10-04'
