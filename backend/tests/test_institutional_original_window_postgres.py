"""Actual advisory-lock exclusion and the observed prior-receipt drift trigger."""
from functools import partial
import io
import json
import time
from types import SimpleNamespace

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from app import db as database
from app.jobs import capture_direct_13f_evidence as capture
from app.jobs import collect_direct_feeds as collector
from app.jobs import collect_direct_13f_priors as priors
from app.jobs import control_institutional_original_window as control
from app.services import institutional_original_window as window
from app.services.direct_13f_priors import _save_attempt
from app.services.direct_feed_store import DirectFeedDocument
from test_direct_13f_preparation_postgres import pg
from test_institutional_original_segments_postgres import seed, freeze


def setup_control(pg,tmp_path,monkeypatch):
    path=tmp_path/'window.json'
    real_create,real_close,real_read=window.create_window,window.close_window,window.read_window
    monkeypatch.setattr(window,'create_window',lambda value,**kw:real_create(value,path,**kw))
    monkeypatch.setattr(window,'close_window',lambda ident:real_close(ident,path))
    monkeypatch.setattr(window,'read_window',lambda **kw:real_read(path,**kw))
    lock=partial(collector.collector_lock,bind=pg)
    monkeypatch.setattr(collector,'collector_lock',lock);monkeypatch.setattr(priors,'collector_lock',lock)
    for key,value in dict(COMBINED_SCHEDULER_ENABLED='true',FLY_MACHINE_ID='fixture',COMBINED_SCHEDULER_MACHINE_ID='fixture',DIRECT_13F_PUBLICATION_ENABLED='false',DIRECT_FEEDS_MODE='shadow').items():monkeypatch.setenv(key,value)
    now=time.time()
    args=['--window-id','a'*32,'--package-sha256',capture.package_hash(),'--approve-source-collection-hold']
    return path,lock,args,now


def test_window_open_cannot_race_existing_collection(pg,tmp_path,monkeypatch,capsys):
    path,lock,args,now=setup_control(pg,tmp_path,monkeypatch)
    with lock() as acquired:
        assert acquired
        control.main(['open',*args,'--approved-at',str(now),'--expires-at',str(now+300)])
        assert json.loads(capsys.readouterr().out)=={'status':'busy','changed':False}
        assert not path.exists()
    control.main(['open',*args,'--approved-at',str(now),'--expires-at',str(now+300)])
    assert json.loads(capsys.readouterr().out)['status']=='opened' and path.exists()
    with pytest.raises(FileExistsError):control.main(['open',*args,'--approved-at',str(now),'--expires-at',str(now+301)])
    control.main(['close',*args]);assert not path.exists()


def test_prior_processing_is_stable_between_pieces_then_resumes(pg,tmp_path,monkeypatch,capsys):
    seed(pg);path,lock,args,now=setup_control(pg,tmp_path,monkeypatch)
    monkeypatch.setattr(database,'engine',pg)
    monkeypatch.setattr(priors,'check_background_job_guard',lambda *a:SimpleNamespace(proceed=True))
    monkeypatch.setattr(priors,'SessionLocal',lambda:Session(pg))
    monkeypatch.setattr(priors,'DirectSourceClient',lambda:object())
    calls=[]
    def processing(db,client,*,document_ids):
        doc=db.get(DirectFeedDocument,document_ids[0]);_save_attempt(doc,{'status':'held'})
        db.commit();calls.append(doc.id);return {'status':'collected'}
    monkeypatch.setattr(priors,'collect_13f_priors',processing)
    with Session(pg) as db:ident=db.scalar(select(DirectFeedDocument.id).order_by(DirectFeedDocument.id))
    prior_args=priors._parser().parse_args(['--document-ids',str(ident)])
    control.main(['open',*args,'--approved-at',str(now),'--expires-at',str(now+300)])
    plan=freeze(pg)
    priors.main(prior_args)
    assert not calls and freeze(pg)==plan
    with lock() as acquired:
        assert acquired
        receipt=capture.export_snapshot(io.BytesIO(),plan['pieces'][0])
    assert receipt['production_writes']==0
    priors.main(prior_args)
    assert not calls and freeze(pg)==plan
    control.main(['close',*args]);assert not path.exists()
    priors.main(prior_args);assert calls==[ident]
    # Keep the original strict safeguard: the previously observed timestamp
    # update must still invalidate an unprotected frozen inventory.
    with pytest.raises(ValueError,match='source_inventory_changed'):
        capture.export_snapshot(io.BytesIO(),plan['pieces'][0])
