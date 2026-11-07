"""A single pool slot remains usable while a complete source is downloaded."""
from datetime import date, timedelta

import pytest
from sqlalchemy import create_engine, select, func, text
from sqlalchemy.orm import Session
from sqlalchemy.pool import QueuePool

from app.db import Base
from app.models import Event, InstitutionalPosition
from app.clients.direct_sources import DirectSourceError
from app.services import direct_feed_collection as collection
from app.services.direct_feed_store import DirectFeedDocument, DirectFeedRevision, discover, utcnow
from test_direct_sec_collection import META, submission


@pytest.mark.parametrize('outcome', ['success', 'denied', 'malformed', 'persistence_failure'])
def test_single_connection_source_wait_preserves_attempts_and_revisions(tmp_path, monkeypatch, outcome):
    engine=create_engine('sqlite:///'+str(tmp_path/'source.sqlite'),poolclass=QueuePool,
                         pool_size=1,max_overflow=0,pool_timeout=.1)
    Base.metadata.create_all(engine)
    kwargs=dict(sources=['sec_13f'],start=date(2026,10,6),end=date(2026,10,6))
    def discovery(db,*args,**kwargs):
        discover(db,'sec_13f',META);db.commit()
        return {'sec_13f':1},[]
    monkeypatch.setattr(collection,'discover_sources',discovery)
    with Session(engine) as db:
        class Client:
            phase='success'
            calls=0
            def get(self,url):
                self.calls+=1
                assert url==META['url']
                assert not db.in_transaction()
                assert engine.pool.checkedout()==0
                # This would time out if source collection retained the only slot.
                with engine.connect() as other:
                    assert other.scalar(text('SELECT 1'))==1
                if self.phase=='denied':raise DirectSourceError('Source HTTP 403: fixture')
                if self.phase=='malformed':return b'incomplete source'
                return submission()
        client=Client()
        first=collection.collect_direct_feeds(db,client,**kwargs)
        assert first['processed']==1 and not first['errors']
        document=db.scalar(select(DirectFeedDocument))
        assert document.attempts==1 and document.status=='parsed'
        source_hash=document.content_hash
        prior_json=document.parsed_json
        document.checked_at=utcnow()-timedelta(days=8);db.commit()
        client.phase=outcome
        original=collection.record_document
        if outcome=='persistence_failure':
            def fail_after_stage(*args,**kwargs):
                original(*args,**kwargs)
                raise ValueError('fixture persistence failure')
            monkeypatch.setattr(collection,'record_document',fail_after_stage)
        result=collection.collect_direct_feeds(db,client,**kwargs)
        db.refresh(document)
        assert document.attempts==2
        assert document.status==('parsed' if outcome=='success' else 'failed')
        assert bool(result['errors'])==(outcome!='success')
        assert document.content_hash==source_hash and document.parsed_json==prior_json
        assert db.scalar(select(func.count()).select_from(DirectFeedRevision))==1
        assert db.scalar(select(func.count()).select_from(Event))==0
        assert db.scalar(select(func.count()).select_from(InstitutionalPosition))==0
        assert client.calls==2
        monkeypatch.setattr(collection,'record_document',original)
        third=collection.collect_direct_feeds(db,client,**kwargs)
        assert third['processed']==0 and client.calls==2
        db.refresh(document);assert document.attempts==2
    engine.dispose()
