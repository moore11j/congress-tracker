from contextlib import nullcontext
import json
from types import SimpleNamespace

import pytest

from app.jobs import capture_direct_13f_evidence as capture
from app.services import institutional_original_window as window
from test_institutional_original_segments import plan


def lease():
    return dict(version=1, window_id='a'*32, package_sha256=capture.package_hash(),
                approved_at=100, expires_at=200, scope=window.SCOPE)


def test_default_off_expiry_and_owned_cleanup(tmp_path):
    path=tmp_path/'window.json';value=lease()
    assert window.read_window(path,now=150) is None
    window.create_window(value,path,now=150,expected_package=capture.package_hash())
    assert window.read_window(path,now=150)==value
    assert window.read_window(path,now=200) is None
    with pytest.raises(FileExistsError):window.create_window(value,path,now=150,expected_package=capture.package_hash())
    with pytest.raises(ValueError,match='not_owned'):window.close_window('b'*32,path)
    assert window.close_window('a'*32,path)==value and not path.exists()
    assert (tmp_path/('institutional-original-copy-'+'a'*32+'.closed.json')).is_file()


@pytest.mark.parametrize('change',['version','id','scope','bool','nan','infinity','duration','reverse','extra'])
def test_invalid_hold_never_created(tmp_path,change):
    value=lease()
    if change=='version':value['version']=True
    if change=='id':value['window_id']='../escape'
    if change=='scope':value['scope']='all_jobs'
    if change=='bool':value['approved_at']=True
    if change=='nan':value['approved_at']=float('nan')
    if change=='infinity':value['expires_at']=float('inf')
    if change=='duration':value['expires_at']=3701
    if change=='reverse':value['expires_at']=50
    if change=='extra':value['renew']=True
    path=tmp_path/'window.json'
    with pytest.raises(ValueError):window.create_window(value,path,now=150,expected_package=capture.package_hash())
    assert not path.exists()


def test_changed_package_future_duplicate_and_oversize_fail_closed(tmp_path):
    path=tmp_path/'window.json';path.write_text(json.dumps(lease()))
    with pytest.raises(ValueError,match='package_changed'):window.read_window(path,now=150,expected_package='0'*64)
    with pytest.raises(ValueError,match='future'):window.read_window(path,now=99)
    path.write_text('{"version":1,"version":1}')
    with pytest.raises(ValueError):window.read_window(path,now=150)
    path.write_bytes(b' '*2049)
    with pytest.raises(ValueError):window.read_window(path,now=150)


@pytest.mark.parametrize('change',['none','missing','id','start','end'])
def test_copy_request_binds_exact_window(tmp_path,monkeypatch,change):
    value=lease()
    request=dict(version=2,request_id='b'*32,package_sha256=capture.package_hash(),
        approved_at=100,expires_at=200,payload='institutional_originals_segment',
        destination='owner_local_onedrive_migration_validation',original_piece=plan()['pieces'][0],source_window_id='a'*32)
    if change=='id':value['window_id']='c'*32
    if change=='start':value['approved_at']=99
    if change=='end':value['expires_at']=201
    monkeypatch.setattr(window,'read_window',lambda **kw:None if change=='missing' else value)
    path=tmp_path/'request.json';path.write_text(json.dumps(request))
    if change=='none':assert capture.read_request(path,now=150)==request
    else:
        with pytest.raises(ValueError,match='missing_exact'):capture.read_request(path,now=150)


@pytest.mark.parametrize('active',[False,True])
def test_mixed_collection_preserves_other_sources(monkeypatch,capsys,active):
    from app.jobs import collect_direct_feeds as job
    monkeypatch.setenv('DIRECT_FEEDS_MODE','shadow')
    monkeypatch.setattr(job,'check_background_job_guard',lambda *a:SimpleNamespace(proceed=True))
    monkeypatch.setattr(job,'collector_lock',lambda **kw:nullcontext(True))
    monkeypatch.setattr(window,'read_window',lambda:lease() if active else None)
    monkeypatch.setattr(job,'ensure_direct_feed_schema',lambda *a:None)
    monkeypatch.setattr(job,'SessionLocal',lambda:nullcontext(object()))
    monkeypatch.setattr(job,'DirectSourceClient',lambda **kw:object())
    seen=[]
    def collect(db,client,**kw):seen.extend(kw['sources']);return {'errors':[]}
    monkeypatch.setattr(job,'collect_direct_feeds',collect)
    job.main(['--sources','sec_13f','sec_form4','--start','2026-11-20','--end','2026-11-20'])
    assert seen==(['sec_form4'] if active else ['sec_13f','sec_form4'])
    report=json.loads(capsys.readouterr().out)
    assert report.get('held_sources')==(['sec_13f'] if active else None)


def test_only_13f_hold_skips_schema_client_and_collection(monkeypatch,capsys):
    from app.jobs import collect_direct_feeds as job
    monkeypatch.setenv('DIRECT_FEEDS_MODE','shadow')
    monkeypatch.setattr(job,'check_background_job_guard',lambda *a:SimpleNamespace(proceed=True))
    monkeypatch.setattr(job,'collector_lock',lambda **kw:nullcontext(True))
    monkeypatch.setattr(window,'read_window',lambda:lease())
    def denied(*a,**kw):raise AssertionError('No source/schema work during hold')
    monkeypatch.setattr(job,'ensure_direct_feed_schema',denied);monkeypatch.setattr(job,'DirectSourceClient',denied)
    job.main(['--sources','sec_13f'])
    assert json.loads(capsys.readouterr().out)['status']=='held'


def test_prior_hold_runs_inside_collection_lock_before_database_or_source(monkeypatch,capsys):
    from app.jobs import collect_direct_13f_priors as job
    monkeypatch.setenv('DIRECT_FEEDS_MODE','shadow');inside=[]
    from contextlib import contextmanager
    @contextmanager
    def lock(**kw):
        inside.append(True)
        try:yield True
        finally:inside.pop()
    monkeypatch.setattr(job,'collector_lock',lock)
    monkeypatch.setattr(job,'check_background_job_guard',lambda *a:SimpleNamespace(proceed=True))
    def held():assert inside;return lease()
    monkeypatch.setattr(window,'read_window',held)
    def denied(*a,**kw):raise AssertionError('No prior write or provider request')
    monkeypatch.setattr(job,'SessionLocal',denied);monkeypatch.setattr(job,'DirectSourceClient',denied)
    job.main(job._parser().parse_args(['--document-ids','22109']))
    assert json.loads(capsys.readouterr().out)['status']=='held'


@pytest.mark.parametrize('change',['approval','package','machine','disabled','publication'])
def test_control_requires_explicit_exact_approval_before_lock(monkeypatch,change):
    from app.jobs import control_institutional_original_window as control
    from app.jobs import collect_direct_feeds as collector
    for key,value in dict(COMBINED_SCHEDULER_ENABLED='true',FLY_MACHINE_ID='fixture',COMBINED_SCHEDULER_MACHINE_ID='fixture',DIRECT_13F_PUBLICATION_ENABLED='false').items():monkeypatch.setenv(key,value)
    args=['open','--window-id','a'*32,'--package-sha256',capture.package_hash(),'--approve-source-collection-hold','--approved-at','100','--expires-at','200']
    if change=='approval':args.remove('--approve-source-collection-hold')
    if change=='package':args[args.index('--package-sha256')+1]='0'*64
    if change=='machine':monkeypatch.setenv('FLY_MACHINE_ID','other')
    if change=='disabled':monkeypatch.setenv('COMBINED_SCHEDULER_ENABLED','false')
    if change=='publication':monkeypatch.setenv('DIRECT_13F_PUBLICATION_ENABLED','true')
    def denied(**kw):raise AssertionError('No lock or write without approval')
    monkeypatch.setattr(collector,'collector_lock',denied)
    with pytest.raises(ValueError,match='exact_source_hold_approval'):control.main(args)
