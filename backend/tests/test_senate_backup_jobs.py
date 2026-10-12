from contextlib import contextmanager
from datetime import date, timedelta
import hashlib
import json
from types import SimpleNamespace

import pytest
from sqlalchemy import select, func

from app.models import Event, EmailDelivery
from app.services.direct_feed_store import DirectFeedRun, DirectFeedRevision, DirectFeedDocument
from app.services.senate_backup_collection import FEED, download_snapshot
from app.services.senate_backup_publication import publish_batch
from app.services.feed_source_control import FeedSourceMismatch
from test_senate_backup_collection import bundle, Response
from test_senate_backup_publication import staged, switch
from test_senate_backup_reconciliation import directory
from test_senate_backup_evidence import NOW
from test_direct_congress_worker import db, counts


@pytest.mark.parametrize('limit',[0,21,True,-1])
def test_batch_rejects_invalid_bound_before_db(limit):
    with pytest.raises(ValueError,match='limit'):publish_batch(None,directory=[],limit=limit)


def test_new_filings_precede_held_retries_and_completed_filings_skip(db,bundle):
    identifier=staged(db,bundle);switch(db)
    first=publish_batch(db,directory=directory(),limit=1,retry_held=True,now=NOW)
    assert first['status']=='partial' and first['processed']==1
    second=publish_batch(db,directory=directory(),limit=1,retry_held=True,now=NOW+timedelta(minutes=1))
    assert second['status']=='ok' and second['inserted_events']==5
    assert second['results'][0]['document_id']==identifier
    third=publish_batch(db,directory=directory(),limit=20,now=NOW+timedelta(minutes=2))
    assert third['processed']==0 and third['inserted_events']==0
    assert counts(db)[:4]==[1,5,5,0]


def test_oldest_held_retry_rotates_without_starving_other_filings(db,bundle):
    staged(db,bundle);switch(db)
    identifiers=[]
    for index in range(3):
        result=publish_batch(db,directory=[],limit=1,retry_held=True,now=NOW+timedelta(minutes=index))
        identifiers.append(result['results'][0]['document_id'])
        assert result['inserted_events']==0
    assert identifiers[0]!=identifiers[1] and identifiers[2]==identifiers[0]


def test_batch_filters_boundary_before_spending_filing_budget(db,bundle):
    identifier=staged(db,bundle);switch(db,boundary=date(2026,10,2))
    result=publish_batch(db,directory=directory(),limit=20,retry_held=True,now=NOW)
    assert result['processed']==1 and result['results'][0]['document_id']!=identifier
    assert result['inserted_transactions']==0


def test_batch_stale_snapshot_or_wrong_owner_cannot_write(db,bundle):
    staged(db,bundle)
    with pytest.raises(FeedSourceMismatch):publish_batch(db,directory=directory(),now=NOW)
    db.rollback();switch(db)
    with pytest.raises(ValueError,match='freshness'):
        publish_batch(db,directory=directory(),now=NOW+timedelta(days=2))
    db.rollback()
    assert counts(db)==[0]*6


def test_batch_rechecks_ownership_between_committed_documents(db,bundle,monkeypatch):
    from app.services import senate_backup_publication as worker
    staged(db,bundle);switch(db)
    original=worker.publish_document
    calls=[]
    def publish(db,identifier,**kwargs):
        result=original(db,identifier,**kwargs);calls.append(identifier)
        if len(calls)==1:switch(db,'paused',1)
        return result
    monkeypatch.setattr(worker,'publish_document',publish)
    with pytest.raises(FeedSourceMismatch):publish_batch(db,directory=directory(),limit=20,now=NOW)
    db.rollback()
    assert len(calls)==1 and counts(db)[:3]==[0,0,0]


def test_collection_job_releases_db_during_http_and_stages_repeat(db,bundle):
    from app.jobs.collect_senate_backup import collect
    calls=[]
    class Http:
        def get(self,url,**kwargs):
            assert not db.in_transaction()
            calls.append(url)
            raw=bundle['manifest'] if url.endswith('snapshot.json') else next(
                raw for path,raw in bundle['files'].items() if url.endswith(path))
            return Response(raw,headers={'X-Repo-Commit':'a'*40})
    first=collect(db,years=[2026],now=NOW,session=Http())
    count=db.scalar(select(func.count()).select_from(DirectFeedRevision));db.rollback()
    second=collect(db,years=[2026],now=NOW,session=Http())
    assert first['filings']==second['filings']==2 and len(calls)==6
    assert db.scalar(select(func.count()).select_from(DirectFeedRevision))==count
    runs=list(db.scalars(select(DirectFeedRun)))
    assert len(runs)==2 and all(run.status=='ok' for run in runs)
    assert all(json.loads(run.report_json)['sources']==[FEED] for run in runs)
    assert db.scalar(select(func.count()).select_from(Event))==0
    assert db.scalar(select(func.count()).select_from(EmailDelivery))==0


def test_collection_failure_records_failure_without_partial_staging(db):
    from app.jobs.collect_senate_backup import collect
    class Http:
        def get(self,*args,**kwargs):raise TimeoutError('isolated failure')
    with pytest.raises(TimeoutError):collect(db,years=[2026],now=NOW,session=Http())
    run=db.scalar(select(DirectFeedRun))
    assert run.status=='failed' and json.loads(run.report_json)['error_type']=='TimeoutError'
    assert db.scalar(select(func.count()).select_from(DirectFeedDocument))==0


@pytest.mark.parametrize('years',[[],[2026,2026],[2024,2025,2026],[True],[2101]])
def test_invalid_years_rejected_before_http(years):
    class Http:
        def get(self,*args,**kwargs):pytest.fail('Invalid scope made a request')
    with pytest.raises(ValueError):download_snapshot(years=years,session=Http())


def test_disabled_standalone_collector_does_not_open_db(monkeypatch,capsys):
    from app.jobs.collect_senate_backup import main
    monkeypatch.delenv('SENATE_BACKUP_COLLECTION_ENABLED',raising=False)
    monkeypatch.setattr('app.db.SessionLocal',lambda:pytest.fail('Disabled job opened DB'))
    main(['--years','2026'])
    assert json.loads(capsys.readouterr().out)['status']=='disabled'


@pytest.mark.parametrize('enabled',[False,True])
def test_existing_publication_entrypoint_requires_backup_flag_and_caps_batch(db,tmp_path,monkeypatch,capsys,enabled):
    from app.jobs import publish_direct_congress as job
    from app.services import senate_backup_publication as worker
    switch(db)
    monkeypatch.setenv('DIRECT_CONGRESS_PUBLICATION_ENABLED','true')
    monkeypatch.setenv('SENATE_BACKUP_PUBLICATION_ENABLED',str(enabled).lower())
    monkeypatch.setattr(job,'SessionLocal',lambda:db)
    monkeypatch.setattr(job,'check_background_job_guard',lambda *a:SimpleNamespace(proceed=True))
    calls=[]
    monkeypatch.setattr(worker,'publish_batch',lambda db,**kwargs:calls.append(kwargs) or {'status':'ok'})
    path=tmp_path/'directory.json';raw=json.dumps(directory()).encode();path.write_bytes(raw)
    job.main(['--feed','senate_ptr','--directory',str(path),'--directory-sha256',hashlib.sha256(raw).hexdigest(),'--limit','100'])
    result=json.loads(capsys.readouterr().out)
    assert result['status']==('ok' if enabled else 'disabled')
    assert len(calls)==int(enabled)
    if enabled:assert calls[0]['limit']==20 and calls[0]['directory']==directory()


@pytest.mark.parametrize('selected,enabled,expected',[
    ('official_senate',False,['house_ptr','senate_ptr']),
    ('official_senate',True,['house_ptr','senate_ptr']),
    ('community_congressional_disclosures',True,['house_ptr'])])
def test_existing_collection_schedule_routes_only_selected_senate_source(db,monkeypatch,capsys,selected,enabled,expected):
    from app.jobs import collect_direct_feeds as job
    from app.jobs import collect_senate_backup as backup
    switch(db,selected)
    monkeypatch.setenv('DIRECT_FEEDS_MODE','shadow')
    monkeypatch.setenv('SENATE_BACKUP_COLLECTION_ENABLED',str(enabled).lower())
    monkeypatch.setattr(job,'SessionLocal',lambda:db)
    monkeypatch.setattr(job,'check_background_job_guard',lambda *a:SimpleNamespace(proceed=True))
    schema_ready=[]
    monkeypatch.setattr(job,'ensure_direct_feed_schema',lambda *a:schema_ready.append(True))
    original_get=db.get
    def guarded_get(*args,**kwargs):
        assert schema_ready, 'Source ownership read before staging bootstrap'
        return original_get(*args,**kwargs)
    monkeypatch.setattr(db,'get',guarded_get)
    @contextmanager
    def lock():yield True
    monkeypatch.setattr(job,'collector_lock',lock)
    calls=[]
    monkeypatch.setattr(job,'collect_direct_feeds',lambda db,client,**kwargs:calls.append(kwargs) or {'errors':[]})
    backup_calls=[]
    monkeypatch.setattr(backup,'collect',lambda db,**kwargs:backup_calls.append(kwargs) or {'status':'staged'})
    job.main(['--sources','house_ptr','senate_ptr','--start','2026-10-01','--end','2026-10-12'])
    assert calls[0]['sources']==expected and len(backup_calls)==int(enabled)
    if enabled:assert backup_calls[0]['years']==[2026]
    assert json.loads(capsys.readouterr().out)['errors']==[]
