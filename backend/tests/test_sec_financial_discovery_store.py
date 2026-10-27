from copy import deepcopy
import json
from sqlalchemy import create_engine,select,func
from sqlalchemy.orm import sessionmaker
import pytest
from app.models import InsightsSnapshot
from app.services import sec_financial_discovery as discovery,sec_financial_discovery_store as store
from test_sec_financial_discovery import source,Client,COMPANY,NOW,DAY,no_network


@pytest.fixture
def database(monkeypatch):
    engine=create_engine('sqlite://')
    InsightsSnapshot.__table__.create(engine)
    factory=sessionmaker(bind=engine)
    monkeypatch.setattr(store,'SessionLocal',factory)
    monkeypatch.setenv('SEC_FINANCIAL_DISCOVERY_ENABLED','1')
    yield factory
    engine.dispose()


def stage(client=None):
    return store.stage(source(COMPANY),symbol='NBIS',cik='0001513845',available_by=DAY,
                       observed_at=NOW,client=client or Client())


def test_persisted_batches_advance_and_repeat_without_extra_writes(database):
    client=Client();counts=[]
    for _ in range(5):
        result=stage(client);counts.append(result['remaining'])
        assert result['checkpoint_written'] and not result['financial_publication_eligible']
    assert counts==[12,9,6,3,0] and len(client.calls)==len(set(client.calls))==15
    saved=store.read('NBIS','0001513845');assert saved==result['state']
    repeat=stage(client);assert not repeat['checkpoint_written'] and len(client.calls)==15
    with database() as db:
        assert db.scalar(select(func.count()).select_from(InsightsSnapshot))==1
        row=db.get(InsightsSnapshot,store.key_for('NBIS','0001513845'))
        assert json.loads(row.payload_json)==saved and len(row.payload_json.encode())<16_000
    # The isolated schema has only the checkpoint table: no canonical/alert/email
    # table exists that this staging service could write as an incidental effect.


def test_disabled_is_no_database_or_transport(monkeypatch):
    monkeypatch.delenv('SEC_FINANCIAL_DISCOVERY_ENABLED',raising=False)
    monkeypatch.setattr(store,'SessionLocal',lambda:pytest.fail('Disabled DB access'))
    assert store.read('NBIS','0001513845') is None
    client=Client()
    with pytest.raises(discovery.DiscoveryError,match='discovery_disabled'):stage(client)
    assert not client.calls


def test_switch_change_during_scan_does_not_write(database,monkeypatch):
    class Disable(Client):
        def get(self,url):
            raw=super().get(url);monkeypatch.setenv('SEC_FINANCIAL_DISCOVERY_ENABLED','0');return raw
    with pytest.raises(discovery.DiscoveryError,match='disabled_during_scan'):stage(Disable())
    with database() as db:assert not db.scalar(select(func.count()).select_from(InsightsSnapshot))


def test_interrupted_batch_retains_exact_checkpoint(database):
    original=stage()['state'];before=deepcopy(original)
    class Failure(Client):
        def get(self,url):
            if self.calls:raise RuntimeError('source unavailable')
            return super().get(url)
    with pytest.raises(RuntimeError,match='source unavailable'):stage(Failure())
    assert store.read('NBIS','0001513845')==before
    assert stage()['remaining']==9


@pytest.mark.parametrize('existing',[False,True])
def test_competing_writer_cannot_be_overwritten(database,existing):
    if existing:stage()
    key=store.key_for('NBIS','0001513845');winner=discovery.advance(source(COMPANY),symbol='NBIS',cik='0001513845',
        available_by=DAY,observed_at=NOW,client=Client(),previous=store.read('NBIS','0001513845'))['state']
    class Race(Client):
        def get(self,url):
            raw=super().get(url)
            if len(self.calls)==1:
                with database() as db:
                    row=db.get(InsightsSnapshot,key)
                    if row is None:row=InsightsSnapshot(kind=key,source='sec_edgar',fetched_at=NOW,payload_json='{}');db.add(row)
                    row.payload_json=discovery._json(winner);db.commit()
            return raw
    with pytest.raises(store.DiscoveryConflict,match='checkpoint_changed'):stage(Race())
    assert store.read('NBIS','0001513845')==winner


@pytest.mark.parametrize('path',['/api/ticker/NBIS','/public/ticker/NBIS','/ticker/NBIS'])
def test_api_request_cannot_start_source_collection(database,monkeypatch,path):
    from app import request_priority
    monkeypatch.setattr(request_priority,'get_request_context',lambda:{'path':path})
    client=Client()
    with pytest.raises(discovery.DiscoveryError,match='worker_only'):stage(client)
    assert not client.calls


@pytest.mark.parametrize('symbol,cik',[('NBIS:OTHER','1513845'),('NB IS','1513845'),('A'*30,'1513845'),('NBIS','0'),('NBIS','bad')])
def test_invalid_checkpoint_keys_rejected_before_queries(database,symbol,cik):
    with pytest.raises(discovery.DiscoveryError,match='checkpoint_identity'):store.read(symbol,cik)


def test_corrupt_null_checkpoint_is_not_treated_as_new_work(database):
    with database() as db:
        db.add(InsightsSnapshot(kind=store.key_for('NBIS','0001513845'),source='sec_edgar',fetched_at=NOW,payload_json='null'));db.commit()
    client=Client()
    with pytest.raises(discovery.DiscoveryError,match='checkpoint_invalid'):stage(client)
    assert not client.calls
