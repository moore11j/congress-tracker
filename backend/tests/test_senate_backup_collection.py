from copy import deepcopy
from datetime import timedelta
import hashlib
import json
import time

import duckdb
import pytest
from sqlalchemy import create_engine, select, func
from sqlalchemy.orm import Session

from app.db import Base
from app.models import Event, EmailDelivery
from app.services import senate_backup_collection as collector
from app.services.direct_feed_store import DirectFeedDocument, DirectFeedRevision
from test_senate_backup_evidence import SNAPSHOT, NOW


@pytest.fixture
def bundle(tmp_path):
    snapshot = deepcopy(SNAPSHOT['snapshot'])
    snapshot['tables'] = {}
    files = {}
    with duckdb.connect(':memory:') as connection:
        for table, records in [('political_filings', SNAPSHOT['filings']),
                               ('political_trades', SNAPSHOT['trades'])]:
            source = tmp_path/(table+'.json')
            source.write_text(json.dumps(records))
            target = tmp_path/(table+'.parquet')
            connection.read_json(str(source)).write_parquet(str(target))
            raw = target.read_bytes()
            path = f'data/{table}/2026-00000-of-00001.parquet'
            files[path] = raw
            snapshot['tables'][table] = {'manifests':[{'year':2026,'files':[{
                'publicPath':path,'size':len(raw),'sha256':hashlib.sha256(raw).hexdigest()}]}]}
    return dict(manifest=json.dumps(snapshot).encode(),files=files,years=[2026],observed_at=NOW.isoformat())


def edit_manifest(bundle, change):
    snapshot = json.loads(bundle['manifest'])
    change(snapshot)
    bundle['manifest'] = json.dumps(snapshot).encode()


def test_actual_parquet_conversion_preserves_complete_rows_and_secondary_holds(bundle, monkeypatch):
    monkeypatch.setattr('requests.sessions.Session.request', lambda *a, **k: pytest.fail('No network'))
    result = collector.decode_bundle(bundle)
    assert len(result)==2 and sum(len(p['transactions']) for p in result)==7
    assert all(not p['publishable'] for p in result)
    assert result==collector.decode_bundle(bundle)


@pytest.mark.parametrize('damage', ['checksum','missing','extra','path','stale','future','duplicate','oversize'])
def test_invalid_snapshot_fails_before_decoder(bundle, monkeypatch, damage):
    if damage=='checksum': bundle['files'][next(iter(bundle['files']))] += b'!'
    if damage=='missing': bundle['files'].pop(next(iter(bundle['files'])))
    if damage=='extra': bundle['files']['extra'] = b'x'
    if damage=='path': edit_manifest(bundle,lambda s:s['tables']['political_filings']['manifests'][0]['files'][0].update(publicPath='../../x'))
    if damage=='stale': bundle['observed_at']=(NOW+timedelta(days=2)).isoformat()
    if damage=='future': bundle['observed_at']=(NOW-timedelta(days=2)).isoformat()
    if damage=='duplicate': edit_manifest(bundle,lambda s:s['tables']['political_filings']['manifests'].append(s['tables']['political_filings']['manifests'][0]))
    if damage=='oversize': edit_manifest(bundle,lambda s:s['tables']['political_filings']['manifests'][0]['files'][0].update(size=collector.MAX_TOTAL+1))
    monkeypatch.setattr(duckdb, 'connect', lambda *a, **k: pytest.fail('Invalid bytes reached decoder'))
    with pytest.raises(ValueError): collector.decode_bundle(bundle)


def test_declared_row_limit_holds_instead_of_silent_truncation(bundle,monkeypatch):
    monkeypatch.setattr(collector,'MAX_TABLE_ROWS',3)
    with pytest.raises(ValueError,match='row budget'): collector.decode_bundle(bundle)


def test_atomic_staging_repeat_and_first_observation(bundle,monkeypatch):
    monkeypatch.setattr('requests.sessions.Session.request',lambda *a, **k:pytest.fail('No network'))
    engine=create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    with Session(engine) as db:
        result=collector.stage_bundle(db,bundle,now=NOW)
        db.commit()
        assert result['filings']==2 and result['rows']==7 and not result['publishable']
        count=lambda model:db.scalar(select(func.count()).select_from(model))
        assert count(DirectFeedDocument)==5 and count(DirectFeedRevision)==5
        original=[r.parsed_json for r in db.scalars(select(DirectFeedDocument).where(DirectFeedDocument.feed==collector.FEED).order_by(DirectFeedDocument.id))]
        assert collector.stage_bundle(db,bundle,now=NOW)==result
        db.commit()
        assert count(DirectFeedDocument)==5 and count(DirectFeedRevision)==5
        bundle['observed_at']=(NOW+timedelta(hours=1)).isoformat()
        collector.stage_bundle(db,bundle,now=NOW+timedelta(hours=1))
        db.commit()
        assert [r.parsed_json for r in db.scalars(select(DirectFeedDocument).where(DirectFeedDocument.feed==collector.FEED).order_by(DirectFeedDocument.id))]==original
        assert count(Event)==0 and count(EmailDelivery)==0
    engine.dispose()


def test_partial_database_failure_rolls_back_entire_snapshot(bundle,monkeypatch):
    from app.services import direct_feed_store as store
    original=store.record_document
    def fail(db,row,*args,**kwargs):
        if row.feed==collector.FEED: raise RuntimeError('Injected storage failure')
        return original(db,row,*args,**kwargs)
    monkeypatch.setattr(store,'record_document',fail)
    engine=create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    with Session(engine) as db:
        with pytest.raises(RuntimeError): collector.stage_bundle(db,bundle,now=NOW)
        assert db.scalar(select(func.count()).select_from(DirectFeedDocument))==0
        assert db.scalar(select(func.count()).select_from(DirectFeedRevision))==0
    engine.dispose()


class Response:
    def __init__(self,raw=b'',status=200,headers=None):
        self.raw,self.status_code,self.headers=raw,status,headers or {}
    def __enter__(self):return self
    def __exit__(self,*args):pass
    def raise_for_status(self):
        if self.status_code>=400:raise RuntimeError('Source denied')
    def iter_content(self,size):yield self.raw


def test_download_pins_all_files_to_manifest_revision(bundle):
    revision='a'*40
    calls=[]
    class Session:
        def get(self,url,**kwargs):
            calls.append(url)
            assert kwargs['stream'] and not kwargs['allow_redirects']
            if url==collector.BASE+'main/snapshot.json':return Response(bundle['manifest'],headers={'X-Repo-Commit':revision})
            assert url.startswith(collector.BASE+revision+'/')
            return Response(bundle['files'][url.removeprefix(collector.BASE+revision+'/')])
    result=collector.download_snapshot(years=[2026],observed_at=NOW,session=Session())
    assert result['files']==bundle['files'] and len(calls)==3
    assert result['repository_revision']==revision


def test_redirect_to_unapproved_host_is_never_requested():
    calls=[]
    class Session:
        def get(self,url,**kwargs):
            calls.append(url)
            return Response(status=302,headers={'Location':'http://127.0.0.1/secret'})
    with pytest.raises(ValueError,match='redirect'):
        collector._get(Session(),collector.BASE+'main/snapshot.json',20,time.monotonic()+10)
    assert len(calls)==1


def test_chunked_response_limit_is_enforced():
    class Session:
        def get(self,*a,**k):return Response(b'x'*21)
    with pytest.raises(ValueError,match='byte limit'):
        collector._get(Session(),collector.BASE+'main/snapshot.json',20,time.monotonic()+10)


def test_decoder_cannot_read_unapproved_files_or_enable_network(tmp_path):
    secret=tmp_path/'unapproved.csv'
    secret.write_text('secret\nno\n')
    with collector._decoder([]) as connection:
        with pytest.raises(duckdb.Error): connection.execute('SELECT * FROM read_csv(?)',[str(secret)])
        with pytest.raises(duckdb.Error): connection.execute('SET enable_external_access=true')
        assert connection.execute("SELECT current_setting('threads')").fetchone()==(1,)


def test_snapshot_that_expires_before_staging_is_rejected(bundle):
    with pytest.raises(ValueError,match='freshness'):
        collector.stage_bundle(None,bundle,now=NOW+timedelta(days=2))


@pytest.mark.parametrize('damage', ['orphan','missing_trade','duplicate_filing','wrong_year','unknown_chamber'])
def test_incomplete_or_contradictory_decoded_populations_fail(bundle,tmp_path,damage):
    table='political_filings' if damage=='duplicate_filing' else 'political_trades'
    rows=deepcopy(SNAPSHOT['filings' if table=='political_filings' else 'trades'])
    if damage=='orphan':rows[0]['docId']='11111111-1111-1111-1111-111111111111'
    if damage=='missing_trade':rows.pop()
    if damage=='duplicate_filing':rows.append(deepcopy(rows[0]))
    if damage=='wrong_year':rows[0]['filingDate']='2025-10-01'
    if damage=='unknown_chamber':rows[0]['chamber']='unknown'
    source=tmp_path/'changed.json';target=tmp_path/'changed.parquet'
    source.write_text(json.dumps(rows))
    with duckdb.connect(':memory:') as connection:
        connection.read_json(str(source)).write_parquet(str(target))
    raw=target.read_bytes();path=f'data/{table}/2026-00000-of-00001.parquet'
    bundle['files'][path]=raw
    edit_manifest(bundle,lambda s:s['tables'][table]['manifests'][0]['files'][0].update(
        size=len(raw),sha256=hashlib.sha256(raw).hexdigest()))
    with pytest.raises(ValueError):collector.decode_bundle(bundle)


@pytest.mark.parametrize('damage',['rollback','conflict'])
def test_new_collection_cannot_restore_older_or_conflicting_snapshot(bundle,damage):
    engine=create_engine('sqlite:///:memory:');Base.metadata.create_all(engine)
    with Session(engine) as db:
        collector.stage_bundle(db,bundle,now=NOW);db.commit()
        before=db.scalar(select(func.count()).select_from(DirectFeedRevision))
        if damage=='rollback':
            def rollback(s):
                from datetime import datetime
                s['generatedAt']=(datetime.fromisoformat(s['generatedAt'].replace('Z','+00:00'))-timedelta(hours=1)).isoformat()
                s['lastSourceCheckAt']=s['generatedAt']
            edit_manifest(bundle,rollback)
        else:
            edit_manifest(bundle,lambda s:s.update(sourceFingerprint='different-same-clock'))
        with pytest.raises(ValueError,match='rollback|conflicting'):
            collector.stage_bundle(db,bundle,now=NOW)
        assert db.scalar(select(func.count()).select_from(DirectFeedRevision))==before
    engine.dispose()
