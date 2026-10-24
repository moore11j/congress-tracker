"""Whole-document selection, immutable peer proof, and source-to-cache parity."""
from copy import deepcopy
from datetime import date,datetime,timezone
from pathlib import Path
import gzip,hashlib,json,socket
from unittest.mock import patch
import pytest
from app.services import sec_tabular_corroboration as corroboration,sec_tabular_reporting as reporting
from app.services import sec_inline_financials as inline,sec_financial_statements as statements,sec_fundamentals as core

NOW=datetime(2026,10,18,6,20,tzinfo=timezone.utc)
CIK='0001793663'

def item(name, values):
    raw=name.encode();source={'cik':CIK,'symbol':'VTEX','filed':'2026-08-14','statement_url':name,'statement_sha256':hashlib.sha256(raw).hexdigest()}
    records={k:{(None,date(2026,6,30)):{'value':v,'unit':'USD'}} for k,v in values.items()}
    return ({'records':records,'evidence':{'source':source,'currency':'USD','accounting_basis':'US GAAP','current_period':'2026-06-30','conflicts':[]}},raw,{})

def test_complete_superset_is_selected_without_splicing_and_order_independently():
    a=item('a',{'revenue':10,'assets':20});b=item('b',{'revenue':10,'assets':20,'cash':-5})
    index,proof=corroboration.select([a,b]);assert index==1 and proof['selected_url']=='b' and proof['values_spliced'] is False
    assert corroboration.select([b,a])==(0,proof)
    assert len(proof['sources'])==2

def test_identical_normalized_documents_have_stable_source_selection():
    a=item('a',{'revenue':0});b=item('b',{'revenue':0})
    assert corroboration.select([b,a])[0]==1

@pytest.mark.parametrize('change',['value','complementary','currency','cik','symbol','filed','period','accounting','conflict','missing','nan','bool','original','duplicate'])
def test_disagreement_identity_missing_or_complementary_fragments_remain_held(change):
    a=item('a',{'revenue':10,'assets':20});b=item('b',{'revenue':10,'assets':20,'cash':-5})
    ev=b[0]['evidence'];point=b[0]['records']['revenue'][None,date(2026,6,30)]
    if change=='value':point['value']=11
    elif change=='complementary':del b[0]['records']['assets']
    elif change=='currency':ev['currency']='CAD'
    elif change in ('cik','symbol','filed'):ev['source'][change]='other'
    elif change=='period':ev['current_period']='2026-03-31'
    elif change=='accounting':ev['accounting_basis']='IFRS'
    elif change=='conflict':ev['conflicts']=[{'reason':'conflict'}]
    elif change=='missing':b[0]['records']['revenue'][None,date(2026,6,30)]=None
    elif change=='nan':point['value']=float('nan')
    elif change=='bool':point['value']=True
    elif change=='original':b=(b[0],b[1]+b'changed',b[2])
    elif change=='duplicate':ev['source']['statement_url']='a'
    with pytest.raises(reporting.TabularReportingError,match='selection_ambiguous'):corroboration.select([a,b])

def originals():
    folder=Path(__file__).parent/'fixtures/sec_interim_statements';urls={}
    for url,meta in json.loads((folder/'sources.json').read_text(encoding='utf-8')).items():
        raw=gzip.decompress((folder/meta['file']).read_bytes())
        assert len(raw)==meta['bytes'] and hashlib.sha256(raw).hexdigest()==meta['sha256'];urls[url]=raw
    return urls[f'https://data.sec.gov/api/xbrl/companyfacts/CIK{CIK}.json'],urls[f'https://data.sec.gov/submissions/CIK{CIK}.json'],urls

class Client:
    issuer_document_urls=frozenset()
    def __init__(self,urls):self.urls=urls;self.calls=[]
    def get(self,url):self.calls.append(url);return self.urls[url]

@pytest.fixture(scope='module')
def acquired():
    facts,company,urls=originals();client=Client(urls)
    with patch.object(socket.socket,'connect',side_effect=AssertionError('No network')):
        supplement=inline.acquire(facts,company,symbol='VTEX',cik=CIK,observed_at=NOW,client=client)
    assert len(client.calls)==len(set(client.calls))==3
    return facts,company,urls,supplement

def test_complete_originals_select_one_source_and_preserve_history_semantics(acquired):
    facts,company,urls,supplement=acquired
    with patch.object(socket.socket,'connect',side_effect=AssertionError('No network')):
        args=dict(symbol='VTEX',cik=CIK,observed_at=NOW,filing_supplement=supplement)
        panel=statements.project(facts,company,**args)
        assert panel==statements.project(facts,company,**args)
        assert panel['summary']['currentRatio']==258444000/88750000
        assert core.project_fundamentals(facts,company,**args)['current_ratio']==panel['summary']['currentRatio']
        assert panel['summary']['revenueTtm'] is None
        assert {r['revenue'] for r in panel['quarterly']}=={64389000,58790000}
        assert all(r['eps'] is None and r['freeCashFlow'] is None for r in panel['quarterly'])
        checked=reporting.validate(supplement,company_raw=company,symbol='VTEX',cik=CIK,available_by=date(2026,10,17))
        proof=supplement['evidence']['table_reporting']['discovery']['corroboration']
        assert checked['evidence']['corroboration']==proof
        assert panel['sourceEvidence']['tableReporting']['corroboration']==proof
        core_proof=json.loads(core.project_fundamentals(facts,company,**args)['source_evidence_json'])
        assert core_proof['table_reporting']['corroboration']==proof
        assert reporting.cached_valid(checked['evidence'],symbol='VTEX',cik=CIK)
        source=checked['evidence']['source']['statement_url']
        assert source.endswith('vtex_2q26_pr_6k.htm')
        assert all(p['source_url']==source for rows in checked['records'].values() for p in rows.values())

@pytest.mark.parametrize('change',['remove_peer','peer_value','peer_identity','proof','selected_value'])
def test_consumer_recomputes_corroborating_originals(acquired,change):
    facts,company,urls,original=acquired;supplement=deepcopy(original)
    peer=supplement['table_corroborating_originals'][0]
    if change=='remove_peer':del supplement['table_corroborating_originals']
    elif change=='peer_value':
        new=peer['raw'].replace(b'64,389',b'64,399');assert new!=peer['raw'];peer['raw']=new
        peer['args']['cover_raw']=new
    elif change=='peer_identity':peer['args']['symbol']='OTHER'
    elif change=='proof':supplement['evidence']['table_reporting']['discovery']['corroboration']['selected_url']='other'
    elif change=='selected_value':
        new=supplement['table_original'].replace(b'64,389',b'64,399');assert new!=supplement['table_original']
        supplement['table_original']=new;supplement['table_args']['cover_raw']=new
    with pytest.raises(ValueError):reporting.validate(supplement,company_raw=company,symbol='VTEX',cik=CIK,available_by=date(2026,10,17))


def test_original_source_cache_reuses_complete_selected_statement(monkeypatch,acquired):
    from sqlalchemy import create_engine,select,func
    from sqlalchemy.orm import sessionmaker
    from app.models import InsightsSnapshot
    from app.services import sec_fundamentals_preparation as prep,sec_directory
    facts,company,urls,supplement=acquired
    proof=supplement['evidence']['table_reporting']['discovery']['corroboration']
    expected_hashes={hashlib.sha256(supplement['table_original']).hexdigest(),
        *(hashlib.sha256(p['raw']).hexdigest() for p in supplement['table_corroborating_originals'])}
    assert {p['source']['statement_sha256'] for p in proof['sources']}==expected_hashes
    client=Client(urls)
    monkeypatch.setattr(socket.socket,'connect',lambda *a,**k:pytest.fail('Unexpected network'))
    monkeypatch.setenv('SEC_FUNDAMENTALS_WARMING_ENABLED','1')
    monkeypatch.setattr(sec_directory,'directory',lambda:{'VTEX':{'cik':CIK}})
    class Clock(datetime):
        @classmethod
        def now(cls,tz=None):return NOW
    engine=create_engine('sqlite://');InsightsSnapshot.__table__.create(engine);factory=sessionmaker(bind=engine)
    try:
        with patch.object(prep,'SessionLocal',factory),patch.object(prep,'datetime',Clock),patch.object(prep,'DirectSourceClient',lambda:client):
            payload=prep.prepare('VTEX');assert payload['values']=={'current_ratio':258444000/88750000}
            assert payload['calculation_evidence']['table_reporting']['corroboration']==proof
            count=len(client.calls);assert prep.prepare('VTEX')==payload and len(client.calls)==count
        with patch.object(statements,'SessionLocal',factory),patch.object(statements,'datetime',Clock),patch.object(statements,'DirectSourceClient',lambda:client):
            panel=statements.prepared('VTEX');assert panel['status']=='partial'
            assert panel['sourceEvidence']['tableReporting']['corroboration']==proof
            count=len(client.calls);assert statements.prepared('VTEX')==panel and len(client.calls)==count
        with factory() as db:
            assert db.scalar(select(func.count()).select_from(InsightsSnapshot))==2
            row=db.get(InsightsSnapshot,'sec-current-fundamentals:VTEX:v1')
            saved=json.loads(row.payload_json)
            saved['calculation_evidence']['table_reporting']['corroboration']['values_spliced']=True
            row.payload_json=json.dumps(saved)
            panel_row=db.get(InsightsSnapshot,'sec-financials:VTEX:v1')
            saved=json.loads(panel_row.payload_json)
            saved['sourceEvidence']['tableReporting']['corroboration']['sources'][0]['source']['statement_sha256']='bad'
            panel_row.payload_json=json.dumps(saved);db.commit()
            assert statements.cached_payload(db,'VTEX',now=NOW) is None
        with patch.object(prep,'SessionLocal',factory):
            assert prep._cached('VTEX',NOW) is None
    finally:engine.dispose()


def test_original_source_score_watchlist_recovery_repeat_crossing_rollback(monkeypatch):
    import test_sec_tagged_foreign_consumers as consumer
    def inputs(symbol):
        assert symbol=='VTEX'
        facts,company,urls=originals()
        return facts,company,CIK,urls
    monkeypatch.setattr(consumer,'originals',inputs)
    monkeypatch.setattr(consumer,'EXPECTED',{'VTEX':{'metrics':{'current_ratio':258444000/88750000}}})
    monkeypatch.setattr(consumer,'NOW',NOW)
    consumer.test_original_source_score_and_alert_recovery_repeat_crossing_rollback('VTEX','current_ratio',monkeypatch)


@pytest.mark.parametrize('change',['missing','one_peer','duplicate','selected','spliced','version','peer_identity','peer_url','peer_hash','selected_hash','count','digest'])
def test_cache_rejects_invalid_corroboration(acquired,change):
    facts,company,urls,supplement=acquired
    # Avoid reparsing complete originals for structural cache-only corruptions.
    proof=json.loads(json.dumps(supplement['evidence']['table_reporting']))
    proof['corroboration']=proof.pop('discovery')['corroboration']
    assert reporting.cached_valid(proof,symbol='VTEX',cik=CIK)
    witness=proof['corroboration'];peer=witness['sources'][0]
    if change=='missing':proof['corroboration']=None
    elif change=='one_peer':witness['sources']=witness['sources'][:1]
    elif change=='duplicate':witness['sources'][1]=deepcopy(peer)
    elif change=='selected':witness['selected_url']='other'
    elif change=='spliced':witness['values_spliced']=True
    elif change=='version':witness['methodology']='other'
    elif change=='peer_identity':peer['source']['symbol']='OTHER'
    elif change=='peer_url':peer['source']['cover_url']='https://example.com/filing'
    elif change=='peer_hash':peer['source']['statement_sha256']='bad'
    elif change=='selected_hash':proof['source']['statement_sha256']='0'*64
    elif change=='count':peer['normalized_observations']=True
    elif change=='digest':peer['normalized_sha256']='bad'
    assert not reporting.cached_valid(proof,symbol='VTEX',cik=CIK)
