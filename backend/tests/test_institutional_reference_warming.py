from contextlib import nullcontext
from datetime import datetime, timedelta, timezone, date
import json
from types import SimpleNamespace
import pytest
from sqlalchemy import create_engine, select, func
from sqlalchemy.orm import sessionmaker
from app.db import Base
from app.models import InsightsSnapshot, InstitutionalPosition, InstitutionalFiling, Event, EmailDelivery
from app.clients.massive_stocks import MassiveStocksError
from app.services.direct_feed_store import DirectFeedDocument, DirectFeedRevision
from app.services.institutional_reference import capture_reference, FEED
from app.jobs import warm_institutional_reference as job
from test_institutional_reference import payload


@pytest.fixture
def setup(monkeypatch):
    engine=create_engine('sqlite:///:memory:');Base.metadata.create_all(engine)
    factory=sessionmaker(bind=engine);clock=[datetime(2026,10,10,6,tzinfo=timezone.utc)];calls=[]
    monkeypatch.setattr(job,'SessionLocal',factory)
    monkeypatch.setattr(job,'_now',lambda:clock[0])
    monkeypatch.setattr(job,'collector_lock',lambda:nullcontext(True))
    monkeypatch.setattr(job,'check_background_job_guard',lambda *a:SimpleNamespace(proceed=True))
    monkeypatch.setenv('INSTITUTIONAL_REFERENCE_WARMING_ENABLED','1')
    monkeypatch.setenv('DIRECT_FEEDS_MODE','shadow')
    monkeypatch.setenv('DIRECT_13F_PUBLICATION_ENABLED','false')
    monkeypatch.setattr('requests.sessions.Session.request',lambda *a,**k:pytest.fail('Unexpected network'))
    class Client:
        def get(self,path,params):
            calls.append(params.copy());return payload()
    monkeypatch.setattr(job,'MassiveStocksClient',Client)
    monkeypatch.setattr(job,'capture_reference',lambda client,**kw:capture_reference(client,**kw,observed_at=clock[0].isoformat()))
    yield factory,clock,calls
    engine.dispose()


def seed(factory,*,key='current',period='2026-09-30',filed='2026-10-08',cusips=('000361105','000361106','000361107'),status='parsed'):
    with factory() as db:
        db.add(DirectFeedDocument(feed='sec_13f',source_key=key,source_url='https://www.sec.gov/fixture',status=status,
            metadata_json='{}',parsed_json=json.dumps({'metadata':{'report_period':period,'filing_date':filed},
            'positions':[{'cusip':c,'shareType':'SH','putCall':''} for c in cusips]})))
        db.commit()


def test_bounded_real_staging_repeats_without_public_records_or_duplicate_requests(setup):
    factory,clock,calls=setup;seed(factory)
    first=job.run()
    assert first['status']=='ok' and first['completed_scopes']==2 and len(calls)==2
    assert first['public_writes']==first['price_requests']==first['emails']==0
    assert job.run()['status']=='cooldown' and len(calls)==2
    clock[0]+=timedelta(minutes=1)
    second=job.run();assert second['completed_scopes']==1 and len(calls)==3
    clock[0]+=timedelta(minutes=1)
    assert job.run()['completed_scopes']==0 and len(calls)==3
    with factory() as db:
        assert db.scalar(select(func.count()).select_from(DirectFeedDocument).where(DirectFeedDocument.feed==FEED))==3
        assert db.scalar(select(func.count()).select_from(DirectFeedRevision))==3
        assert db.scalar(select(func.count()).select_from(Event))==0
        assert db.scalar(select(func.count()).select_from(EmailDelivery))==0
        state=json.loads(db.get(InsightsSnapshot,job.KEY).payload_json)
        assert state['universe_prepared_at']=='2026-10-10T06:00:00+00:00'


def test_scope_respects_historical_identifier_availability_and_source_status(setup):
    factory,clock,calls=setup
    seed(factory,cusips=('000361105','000361106'))
    seed(factory,key='old',period='2025-12-31',filed='2026-02-08',cusips=('000361108',))
    seed(factory,key='held',cusips=('000361109',),status='quarantined')
    with factory() as db:
        for cusip,filed in [('000361105',date(2026,9,1)),('000361106',date(2026,10,9))]:
            filing=InstitutionalFiling(cik='0000000001',accession_number='mapping-'+cusip,report_year=2026,report_quarter=2,filing_date=filed)
            db.add(filing);db.flush()
            db.add(InstitutionalPosition(filing_id=filing.id,cik='0000000001',cusip=cusip,normalized_symbol='AAA',symbol='AAA',
                report_year=2026,report_quarter=2,filing_date=filed,shares=1,value_usd=1))
        db.commit()
    result=job.run()
    assert [r['cusip'] for r in result['results']]==['000361106']
    assert len(calls)==1


def test_quota_refusal_stops_batch_and_has_persistent_recovery_cooldown(setup,monkeypatch):
    factory,clock,calls=setup;seed(factory)
    client=job.MassiveStocksClient()
    def denied(path,params):calls.append(params);raise MassiveStocksError('provider_429')
    monkeypatch.setattr(client,'get',denied)
    original=job.MassiveStocksClient;monkeypatch.setattr(job,'MassiveStocksClient',lambda:client)
    result=job.run();assert result['status']=='partial' and len(calls)==1
    clock[0]+=timedelta(minutes=1)
    assert job.run()['status']=='cooldown' and len(calls)==1
    clock[0]+=timedelta(minutes=5)
    monkeypatch.setattr(job,'MassiveStocksClient',original)
    assert job.run()['completed_scopes']==2 and len(calls)==3


def test_disabled_pressure_or_busy_job_does_not_plan_or_fetch(setup,monkeypatch):
    factory,clock,calls=setup
    monkeypatch.setattr(job,'SessionLocal',lambda:pytest.fail('Database should not open'))
    monkeypatch.setenv('INSTITUTIONAL_REFERENCE_WARMING_ENABLED','0')
    assert job.run()['status']=='disabled'
    monkeypatch.setenv('INSTITUTIONAL_REFERENCE_WARMING_ENABLED','1')
    monkeypatch.setattr(job,'collector_lock',lambda:nullcontext(False))
    assert job.run()['status']=='busy'
    monkeypatch.setattr(job,'check_background_job_guard',lambda *a:SimpleNamespace(proceed=False,to_dict=lambda:{'reason':'pressure'}))
    assert job.run()=={'status':'held','reason':'pressure'}
    assert calls==[]


def test_new_sources_wait_for_bounded_plan_refresh_and_truncation_is_explicit(setup,monkeypatch):
    factory,clock,calls=setup;seed(factory,cusips=('000361105',))
    assert job.run()['completed_scopes']==1
    seed(factory,key='new',cusips=('000361106',))
    clock[0]+=timedelta(minutes=1)
    assert job.run()['completed_scopes']==0
    clock[0]+=timedelta(minutes=15)
    assert job.run()['completed_scopes']==1
    monkeypatch.setattr(job,'MAX_DOCUMENTS',1)
    with factory() as db:
        plan=job.plan_scopes(db,clock[0],{})
        assert plan['universe_truncated'] is True


def test_verified_identities_leave_queue_on_refresh_so_bounded_scope_can_advance(setup,monkeypatch):
    factory,clock,calls=setup
    seed(factory,cusips=('000361105','000361106','000361107'))
    monkeypatch.setattr(job,'MAX_SCOPES',1)
    assert job.run()['completed_scopes']==1
    clock[0]+=timedelta(minutes=16)
    assert job.run()['completed_scopes']==1
    clock[0]+=timedelta(minutes=16)
    assert job.run()['completed_scopes']==1
    assert {r['cusip'] for r in calls}=={'000361105','000361106','000361107'}
    clock[0]+=timedelta(minutes=16)
    result=job.run()
    assert result['pending_scopes']==0 and result['completed_scopes']==0
    assert result['universe_truncated'] is False


def test_large_current_backlog_cannot_starve_prior_quarter_and_repeat_advances(setup):
    factory,clock,calls=setup
    seed(factory,cusips=tuple(f'{n:09d}' for n in range(1,101)))
    seed(factory,key='prior',period='2026-06-30',filed='2026-08-08',cusips=('000000201','000000202'))
    # Repeated holdings prioritize the widely held identity within its quarter.
    seed(factory,key='current-peer',cusips=('000000001',))
    first=job.run()
    assert [(r['cusip'],r['report_period']) for r in first['results']]==[
        ('000000001','2026-09-30'),('000000202','2026-06-30')]
    assert first['completed_scopes']==2 and first['pending_scopes']==102
    clock[0]+=timedelta(minutes=1)
    second=job.run()
    assert [(r['cusip'],r['report_period']) for r in second['results']]==[
        ('000000100','2026-09-30'),('000000201','2026-06-30')]
    assert len(calls)==4 and len({(r['cusip'],r['date']) for r in calls})==4
    clock[0]+=timedelta(minutes=1)
    third=job.run()
    assert third['completed_scopes']==2
    assert {r['report_period'] for r in third['results']}=={'2026-09-30'}
    assert all(r['public_writes']==r['price_requests']==r['emails']==0 for r in [first,second,third])


def test_daily_deferred_prior_does_not_waste_current_quarter_capacity(setup):
    factory,clock,calls=setup
    seed(factory,cusips=('000000101','000000102','000000103'))
    seed(factory,key='prior',period='2026-06-30',filed='2026-08-08',cusips=('000000201',))
    with factory() as db:
        plan=job.plan_scopes(db,clock[0],{'000000201:2026-06-30':clock[0].isoformat()})
    assert len(plan['scopes'])==2 and plan['deferred_scopes']==1
    assert {r['report_period'] for r in plan['scopes']}=={'2026-09-30'}
    assert calls==[]
