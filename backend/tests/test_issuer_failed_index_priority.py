"""Failed current indexes recover within the existing daily/shared request budget."""
from datetime import datetime,timedelta,timezone
import json,socket

import pytest
from sqlalchemy import create_engine,select,func
from sqlalchemy.orm import Session
from issuer_fixture_clock import source_clock
from app.db import Base
from app.clients.direct_sources import DirectSourceError
from app.services import issuer_discovery as discovery,issuer_transcripts as issuer,direct_feed_store as store
from app.services.direct_feed_store import DirectFeedDocument as Doc,DirectFeedRevision
from test_issuer_reddit_content_link import original,company,URL

NOW=datetime(2026,11,7,6,13,tzinfo=timezone.utc)


@pytest.fixture
def setup(monkeypatch):
    monkeypatch.setattr(socket.socket,'connect',lambda *a,**k:pytest.fail('Unexpected network'))
    monkeypatch.setattr(discovery,'utcnow',lambda:NOW-timedelta(hours=40))
    engine=create_engine('sqlite://');Base.metadata.create_all(engine)
    with Session(engine) as db:
        c=company();raw=original('index-2026.json')
        candidates=discovery.parse_index(raw,c,2026,NOW-timedelta(hours=40))
        current=store.discover(db,discovery.INDEX_FEED,discovery.index_metadata(c,2026))
        store.record_document(db,current,raw,raw.decode(),{'candidates':candidates});db.commit()
        current.status='failed';current.checked_at=NOW-timedelta(hours=25)
        current.error='DirectSourceError: Unreviewed issuer transcript link or type'
        prior=store.discover(db,discovery.INDEX_FEED,discovery.index_metadata(c,2025))
        prior.status='parsed';prior.checked_at=NOW
        old=store.discover(db,'issuer_earnings',candidates[-1])
        old.status='parsed';old.checked_at=NOW-timedelta(hours=30)
        db.commit();monkeypatch.setattr(discovery,'utcnow',lambda:NOW)
        yield db,c,current,prior,old
    engine.dispose()


class Client:
    issuer_document_urls=frozenset()
    def __init__(self,db,company,*,deny=False):
        self.db=db;self.calls=[];self.deny=deny
        self.pages={discovery.index_url(company,2026):original('index-2026.json'),URL:original('transcript-q3-2026.pdf')}
    def get(self,url):
        assert not self.db.in_transaction()
        self.calls.append(url)
        if self.deny:raise DirectSourceError('Source HTTP 429: synthetic cooldown')
        assert url in self.pages,'Historical PDF should not precede current recovery'
        return self.pages[url]


def test_complete_current_recovery_precedes_old_pdf_and_keeps_identity(setup):
    db,c,current,prior,old=setup;client=Client(db,c)
    before=(old.checked_at,old.attempts,old.status)
    result=discovery.collect_discovery(db,client,[c],limit=99,recheck_hours=1)
    assert result['attempted']==result['processed']==2 and not result['errors']
    assert client.calls==list(client.pages)
    db.refresh(old);assert (old.checked_at,old.attempts,old.status)==before
    prepared=issuer.prepared_transcript(db,'RDDT',observed_by=NOW)
    assert prepared['status']=='prepared' and prepared['quarter']==3 and prepared['year']==2026
    assert prepared['published_date']=='2026-10-29' and prepared['has_qa']
    assert issuer.prepared_transcript(db,'RDDT',observed_by=NOW)==prepared
    assert db.scalar(select(func.count()).select_from(Doc).where(Doc.source_key=='RDDT:earnings_transcript:2026:Q3'))==1
    assert db.scalar(select(func.count()).select_from(DirectFeedRevision).where(DirectFeedRevision.document_id==current.id))==1


@pytest.mark.parametrize('scenario,expected',[
    ('due','current'),('not_due','old'),('longer_recheck','old'),
    ('healthy','old'),('historical_failure','old'),('aging_index','prior'),
    ('new_index','prior'),('quarantined','old'),
])
def test_eligibility_existing_aging_priority_and_archive_order_are_preserved(setup,scenario,expected):
    db,c,current,prior,old=setup;recheck=24
    if scenario=='not_due':current.checked_at=NOW-timedelta(hours=23,minutes=59)
    elif scenario=='longer_recheck':recheck=48;old.checked_at=NOW-timedelta(hours=50)
    elif scenario=='healthy':current.status='parsed'
    elif scenario=='historical_failure':
        current.status='parsed';current.checked_at=NOW
        prior.status='failed';prior.checked_at=NOW-timedelta(hours=25)
    elif scenario=='aging_index':prior.checked_at=NOW-timedelta(hours=36)
    elif scenario=='new_index':prior.checked_at=None
    elif scenario=='quarantined':current.error='Issuer period changed source URL; review required'
    db.commit();client=Client(db,c,deny=True)
    result=discovery.collect_discovery(db,client,[c],limit=2,recheck_hours=recheck)
    assert result['attempted']==1 and result['stop_reason'].startswith('Source HTTP 429')
    assert client.calls==[{'current':current,'prior':prior,'old':old}[expected].source_url]


def test_failed_current_attempt_does_not_retry_before_daily_boundary(setup):
    db,c,current,prior,old=setup;client=Client(db,c,deny=True)
    discovery.collect_discovery(db,client,[c],limit=2,recheck_hours=1)
    assert client.calls==[current.source_url]
    discovery.collect_discovery(db,client,[c],limit=2,recheck_hours=1)
    assert client.calls.count(current.source_url)==1 and current.attempts==1
