"""Complete originals preserve financial bytes across SEC delivery-path rotation."""
from copy import deepcopy
from datetime import timedelta
import gzip,hashlib,json
import pytest
from app.services import sec_financial_discovery as discovery,sec_financial_discovery_store as store
from app.services import sec_financial_discovery_reader as reader,sec_original_integrity as integrity
from app.clients.direct_sources import DirectSourceError
from test_sec_financial_discovery import ROOT,source,COMPANY,FACTS,COVER,DAY,no_network
from test_sec_financial_discovery_progress import Client,progress,single_directory,NOW
from test_sec_financial_discovery_store import database

def rotated(raw,suffix='one'):
    match=integrity._TERMINAL.search(raw)
    if match is None:return raw
    start,end=match.span('path')
    return raw[:start]+('/different/SEC/delivery/path/'+suffix).encode()+raw[end:]

class RotatingClient(Client):
    def get(self,url):return rotated(super().get(url),str(len(self.calls)))

def test_actual_october31_cover_has_only_reviewed_terminal_path_drift():
    url='https://www.sec.gov/Archives/edgar/data/1513845/000110465926112824/tm2626792d1_6k.htm'
    old=source(url);new=gzip.decompress((ROOT/'delivery-2026-10-31.htm.gz').read_bytes())
    assert hashlib.sha256(old).hexdigest()=='8e35430b9a7b2befe783c5de1e51528be08b6b09ca0841df60bc60bc27bbeffe'
    assert hashlib.sha256(new).hexdigest()=='058e1e1657fd0078ced08923da1e37134ab2684b28271efa295e3a64857d6be9'
    assert integrity.document_digest(old)==integrity.document_digest(new)
    proof={'sha256':hashlib.sha256(old).hexdigest(),**discovery.document_proof(old)}
    assert integrity.verified_original(new,proof)['source_sha256']==hashlib.sha256(new).hexdigest()

def test_full_actual_cohort_qualifies_and_restores_with_rotating_delivery_paths(database):
    phases=[]
    for _ in range(22):
        client=RotatingClient();result=progress(client)
        assert len(client.calls)<=6 and len(client.calls)==len(set(client.calls))
        phases.append(result['phase'])
        if result['phase']=='complete':break
    assert phases==['covers']*5+['qualification']*15+['complete']
    assert result['status']=='ready_for_original_revalidation'
    checkpoint=store.read('NBIS','0001513845');client=RotatingClient()
    args=dict(symbol='NBIS',cik='0001513845',available_by=DAY,observed_at=NOW)
    restored=reader.restore(source(COMPANY),source(FACTS),checkpoint=checkpoint,client=client,**args)
    from app.services import sec_fundamentals as core
    values=core.project_fundamentals(source(FACTS),source(COMPANY),symbol='NBIS',cik='0001513845',observed_at=NOW,filing_supplement=restored)
    assert values['current_ratio']==pytest.approx(4.029839487029043)
    stable=reader.restore(source(COMPANY),source(FACTS),checkpoint=checkpoint,client=Client(),**args)
    stable_values=core.project_fundamentals(source(FACTS),source(COMPANY),symbol='NBIS',cik='0001513845',observed_at=NOW,filing_supplement=stable)
    assert {k:v for k,v in values.items() if k!='source_evidence_json'}=={k:v for k,v in stable_values.items() if k!='source_evidence_json'}
    assert len(client.calls)==4 and not result['financial_publication_eligible']
    before=deepcopy(checkpoint);repeat=progress(RotatingClient())
    assert repeat['phase']=='complete' and not repeat['checkpoint_written']
    assert store.read('NBIS','0001513845')==before

def test_single_qualified_statement_restores_different_raw_hashes_same_content(database):
    company=single_directory()
    assert progress(company=company)['phase']=='covers'
    assert progress(company=company)['phase']=='qualification'
    checkpoint=store.read('NBIS','0001513845')
    args=dict(symbol='NBIS',cik='0001513845',available_by=DAY,observed_at=NOW,checkpoint=checkpoint)
    assert reader.restore(company,source(FACTS),client=RotatingClient(),**args)
    class Changed(Client):
        def get(self,url):return super().get(url)+b' '
    with pytest.raises(discovery.DiscoveryError,match='qualified_original_changed'):
        reader.restore(company,source(FACTS),client=Changed(),**args)

def legacy_checkpoint(company):
    progress(company=company)
    key=store.key_for('NBIS','0001513845');state,expected=store._read(key)
    for r in state['receipts'].values():
        r.pop('document_sha256',None);r.pop('document_integrity_policy',None)
    store._persist(key,state,expected,NOW)

def test_legacy_pin_reobserves_without_admitting_values_then_qualifies(database):
    company=single_directory();legacy_checkpoint(company)
    client=RotatingClient();result=progress(client,company=company)
    assert result['phase']=='cover_reobserved' and result['status']=='pending'
    assert len(client.calls)==2 and not result['financial_publication_eligible']
    hint=next(iter(store.read('NBIS','0001513845')['receipts'].values()))
    assert hint['document_integrity_policy']==integrity.POLICY and 'qualification' not in hint
    next_result=progress(RotatingClient(),company=company,now=NOW+timedelta(minutes=20))
    assert next_result['phase']=='qualification' and next_result['qualification_status']=='qualified'

def test_legacy_reobservation_denial_preserves_checkpoint(database):
    company=single_directory();legacy_checkpoint(company);before=store.read('NBIS','0001513845')
    class Denied(RotatingClient):
        def get(self,url):
            if self.calls:raise DirectSourceError('Source HTTP 403')
            return super().get(url)
    with pytest.raises(DirectSourceError,match='403'):progress(Denied(),company=company)
    assert store.read('NBIS','0001513845')==before

def test_known_content_change_never_reobserved_as_delivery_drift(database):
    company=single_directory();progress(company=company);before=store.read('NBIS','0001513845')
    class Changed(Client):
        def get(self,url):return super().get(url)+b' '
    client=Changed()
    with pytest.raises(discovery.DiscoveryError,match='cover_document_changed'):
        progress(client,company=company)
    assert len(client.calls)==1 and store.read('NBIS','0001513845')==before

@pytest.mark.parametrize('bad',[{'document_sha256':'a'*64}, {'document_integrity_policy':integrity.POLICY},
    {'document_sha256':'z'*64,'document_integrity_policy':integrity.POLICY},
    {'document_sha256':'a'*64,'document_integrity_policy':'ignore_all_scripts'}])
def test_malformed_document_proofs_fail_closed(bad):
    assert not discovery.valid_document_proof(bad)


def test_raw_identical_legacy_cover_gains_pin_before_qualification(database):
    company=single_directory();legacy_checkpoint(company)
    assert progress(company=company)['phase']=='cover_reobserved'
    assert progress(RotatingClient(),company=company)['qualification_status']=='qualified'
    checkpoint=store.read('NBIS','0001513845')
    assert reader.restore(company,source(FACTS),symbol='NBIS',cik='0001513845',available_by=DAY,
        observed_at=NOW,checkpoint=checkpoint,client=RotatingClient())
