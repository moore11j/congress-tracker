"""Actual complete filings preserve results under shared qualification budgets."""
from copy import deepcopy
from types import SimpleNamespace
import pytest
from sqlalchemy import delete
from app.models import InsightsSnapshot
from app.services import sec_financial_discovery as discovery,sec_financial_discovery_store as store
from app.services import sec_financial_discovery_reader as reader
from app.clients.direct_sources import DirectSourceError
from test_sec_financial_discovery import source,COMPANY,FACTS,DAY,no_network
from test_sec_financial_discovery_progress import Client,progress,NOW
from test_sec_financial_discovery_store import database

def frozen_clock(monkeypatch):
    monkeypatch.setattr(store,'time',SimpleNamespace(monotonic=lambda:0,process_time=lambda:0))

def all_covers():
    for _ in range(5):assert progress()['phase']=='covers'

def finish():
    runs=[]
    for _ in range(22):
        client=Client();result=progress(client);runs.append((result,list(client.calls)))
        assert len(client.calls)<=6
        if result['phase']=='qualification':
            assert result['inspected']==len(result['qualifications'])<=3
            assert result['original_requests']==len(client.calls)
        if result['phase']=='complete':return runs
    pytest.fail('Full complete cohort did not finish')

def test_complete_actual_cohort_matches_one_filing_baseline(database,monkeypatch):
    frozen_clock(monkeypatch)
    monkeypatch.setattr(store,'QUALIFICATION_BATCH_LIMIT',1)
    baseline=finish();expected=deepcopy(store.read('NBIS','0001513845'))
    args=dict(symbol='NBIS',cik='0001513845',available_by=DAY,observed_at=NOW)
    original=reader.restore(source(COMPANY),source(FACTS),checkpoint=expected,client=Client(),**args)
    with database() as db:db.execute(delete(InsightsSnapshot));db.commit()
    monkeypatch.setattr(store,'QUALIFICATION_BATCH_LIMIT',3)
    batches=finish();actual=store.read('NBIS','0001513845')
    assert actual==expected
    assert reader.restore(source(COMPANY),source(FACTS),checkpoint=actual,client=Client(),**args)==original
    assert len(batches)<len(baseline)
    assert sum(r['inspected'] for r,c in batches if r['phase']=='qualification')==15
    before=deepcopy(actual);client=Client();repeat=progress(client)
    assert repeat['phase']=='complete' and not repeat['checkpoint_written'] and not client.calls
    assert store.read('NBIS','0001513845')==before

@pytest.mark.parametrize('kind',['wall','cpu'])
def test_continuation_allowance_stops_before_next_filing(database,monkeypatch,kind):
    all_covers();clock=[0]
    monkeypatch.setattr(store,'time',SimpleNamespace(monotonic=lambda:clock[0] if kind=='wall' else 0,
        process_time=lambda:clock[0] if kind=='cpu' else 0))
    real=reader.inspect_candidate
    def inspect(*args,**kwargs):
        result=real(*args,**kwargs);clock[0]=5 if kind=='wall' else 2;return result
    monkeypatch.setattr(reader,'inspect_candidate',inspect)
    result=progress()
    assert result['inspected']==1 and result['qualification_stop_reason']=='continuation_budget'

def test_later_source_denial_keeps_entire_prior_checkpoint(database,monkeypatch):
    all_covers();frozen_clock(monkeypatch);before=deepcopy(store.read('NBIS','0001513845'))
    real=reader.inspect_candidate;count=[0]
    def inspect(*args,**kwargs):
        count[0]+=1
        if count[0]==2:raise DirectSourceError('Source HTTP 403')
        return real(*args,**kwargs)
    monkeypatch.setattr(reader,'inspect_candidate',inspect)
    with pytest.raises(DirectSourceError,match='403'):progress()
    assert count[0]==2 and store.read('NBIS','0001513845')==before

def test_budget_exhaustion_never_commits_incomplete_candidate(database,monkeypatch):
    all_covers();frozen_clock(monkeypatch);real=reader.inspect_candidate;count=[0];first=[]
    def inspect(*args,**kwargs):
        count[0]+=1
        if count[0]==2:
            # Exercise the actual transport guard before a seventh request.
            url=next(iter(kwargs['checkpoint']['receipts'].values()))['candidate']['cover_url']
            for _ in range(7):kwargs['client'].get(url)
            pytest.fail('Seventh source request escaped shared budget')
        result=real(*args,**kwargs);first.append(result['receipt']['candidate']['accession']);return result
    monkeypatch.setattr(reader,'inspect_candidate',inspect)
    client=Client();result=progress(client)
    assert len(client.calls)==6 and result['inspected']==1
    assert result['qualification_stop_reason']=='original_budget'
    state=store.read('NBIS','0001513845')
    assert [r['candidate']['accession'] for r in state['receipts'].values() if 'qualification' in r]==first

def test_later_legacy_cover_is_deferred_to_separate_phase(database,monkeypatch):
    all_covers();frozen_clock(monkeypatch);real=reader.inspect_candidate;count=[0]
    def inspect(*args,**kwargs):
        count[0]+=1
        if count[0]==2:raise discovery.DiscoveryError('discovery_cover_changed')
        return real(*args,**kwargs)
    monkeypatch.setattr(reader,'inspect_candidate',inspect)
    result=progress()
    assert result['inspected']==1 and result['qualification_stop_reason']=='legacy_cover_reobservation'

def test_mid_batch_switch_off_does_not_overwrite(database,monkeypatch):
    all_covers();frozen_clock(monkeypatch);before=deepcopy(store.read('NBIS','0001513845'))
    real=reader.inspect_candidate
    def inspect(*args,**kwargs):
        result=real(*args,**kwargs);monkeypatch.setenv('SEC_FINANCIAL_DISCOVERY_ENABLED','0');return result
    monkeypatch.setattr(reader,'inspect_candidate',inspect)
    with pytest.raises(discovery.DiscoveryError,match='disabled_during_scan'):progress()
    monkeypatch.setenv('SEC_FINANCIAL_DISCOVERY_ENABLED','1')
    assert store.read('NBIS','0001513845')==before


def test_concurrent_checkpoint_winner_is_not_overwritten(database,monkeypatch):
    all_covers();frozen_clock(monkeypatch)
    key=store.key_for('NBIS','0001513845');winner,expected=store._read(key)
    winner['concurrent_marker']='retained'
    real=reader.inspect_candidate;count=[0]
    def inspect(*args,**kwargs):
        result=real(*args,**kwargs);count[0]+=1
        if count[0]==1:store._persist(key,winner,expected,NOW)
        return result
    monkeypatch.setattr(reader,'inspect_candidate',inspect)
    with pytest.raises(store.DiscoveryConflict,match='checkpoint_changed'):progress()
    assert store.read('NBIS','0001513845')==winner
