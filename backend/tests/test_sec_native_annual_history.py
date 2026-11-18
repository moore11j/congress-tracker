from copy import deepcopy
from datetime import datetime,timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
import gzip,hashlib,json,socket
import pytest
from app.services import sec_native_annual_history as annual,sec_inline_financials as inline
from app.services import sec_fundamentals as sec,sec_statement_currency as native

ROOT=Path(__file__).parent/'fixtures/sec_native_annual_history'
NOW=datetime(2026,11,16,4,30,tzinfo=timezone.utc)
CIK='0001879248'


def originals():
    manifest=json.loads((ROOT/'sources.json').read_bytes());raw={}
    for name,meta in manifest.items():
        raw[name]=gzip.decompress((ROOT/meta['file']).read_bytes())
        assert len(raw[name])==meta['bytes'] and hashlib.sha256(raw[name]).hexdigest()==meta['sha256']
    return manifest,raw


def acquire(raw,manifest):
    urls={meta['url']:raw[name] for name,meta in manifest.items()};calls=[]
    class Client:
        issuer_document_urls=frozenset()
        def get(self,url):calls.append(url);return urls[url]
    with (patch.object(socket.socket,'connect',lambda *a,**k:pytest.fail('Unexpected network')),
          patch.dict('os.environ',{'SEC_FINANCIAL_DISCOVERY_READ_ENABLED':'0'})):
        result=inline.acquire(raw['facts'],raw['company'],symbol='ADSE',cik=CIK,observed_at=NOW,client=Client())
    return result,calls


@pytest.fixture(scope='module')
def acquired():
    manifest,raw=originals();supplement,calls=acquire(raw,manifest)
    return manifest,raw,supplement,calls


def project(raw,supplement):
    return sec.project_fundamentals(raw['facts'],raw['company'],symbol='ADSE',cik=CIK,observed_at=NOW,filing_supplement=supplement)


def test_complete_originals_recover_six_ratios_without_usd_or_roe_inference(acquired):
    manifest,raw,supplement,calls=acquired;value=project(raw,supplement);proof=json.loads(value['source_evidence_json'])
    assert calls==[manifest[k]['url'] for k in ('cover','current','annual')]
    assert proof['current']['revenue']['value']==24297000
    assert proof['prior']['revenue']['value']==45364000
    expected=dict(revenue_growth=(24297000/45364000-1)*100,gross_margin=-16466000/24297000*100,
        operating_margin=-52931000/24297000*100,net_margin=-82449000/24297000*100,
        operating_margin_expansion=(-52931000/24297000+33665000/45364000)*100,fcf_margin=-28493000/24297000*100)
    assert {k:value[k] for k in expected}==pytest.approx(expected)
    assert value['current_ratio']==pytest.approx(0.9362844043874885)
    assert not {'free_cash_flow','roe','net_debt_to_ebitda','ev_to_ebitda'} & value.keys()
    assert proof['native_cash_flow']['currency']=='EUR' and proof['native_cash_flow']['current']==-28493000
    assert proof['native_cash_flow']['canonical_usd_amount_available'] is False
    from app.services.fundamentals_cache import fundamentals_summary_from_cache_row
    summary=fundamentals_summary_from_cache_row(SimpleNamespace(**value),now=NOW)
    assert summary['status']=='unavailable'
    from app.services.confirmation_score import _fundamentals_context_source
    assert _fundamentals_context_source(summary)==_fundamentals_context_source(None)
    assert annual.cached_valid(proof) and native.cached_currency_valid(proof)
    assert project(raw,supplement)==value
    assert acquire(raw,manifest)==(supplement,calls)


def test_historical_panel_keeps_native_annual_amounts_and_original_provenance(acquired):
    from app.services import sec_financial_statements as statements
    _,raw,supplement,_=acquired
    args=dict(symbol='ADSE',cik=CIK,observed_at=NOW,filing_supplement=supplement)
    panel=statements.project(raw['facts'],raw['company'],**args)
    assert any(p['date']=='2025-12-31' and p['revenue']==31559000 for p in panel['annual'])
    assert statements.project(raw['facts'],raw['company'],**args)==panel
    row=SimpleNamespace(source='sec_edgar',fetched_at=NOW,payload_json=json.dumps(panel))
    assert statements.cached_row_payload(row,'ADSE',now=NOW)==panel
    old=deepcopy(panel);old['sourceEvidence'].pop('nativeAnnualHistoryVersion')
    row.payload_json=json.dumps(old)
    assert statements.cached_row_payload(row,'ADSE',now=NOW) is None


@pytest.mark.parametrize('defect',['amount','source_hash','cik','currency','future_file','saved_proof','nested','missing'])
def test_changed_annual_source_or_proof_is_rejected(acquired,defect):
    _,raw,supplement,_=deepcopy(acquired);history=supplement['annual_history']
    if defect=='amount':history['facts']['Revenue']['units']['EUR'][0]['val']+=1
    elif defect=='source_hash':history['evidence']['sha256']='0'*64
    elif defect=='cik':history['evidence']['cik']='0000000001'
    elif defect=='currency':history['evidence']['currency']='USD'
    elif defect=='future_file':history['evidence']['filed']='2026-12-31'
    elif defect=='saved_proof':supplement['evidence']['native_annual_history']['annual_revenue']+=1
    elif defect=='nested':history['annual_history']={}
    elif defect=='missing':supplement.pop('annual_history')
    with pytest.raises(ValueError):project(raw,supplement)


@pytest.mark.parametrize('defect',['amount','missing','fingerprint','source_point'])
def test_saved_cache_rejects_changed_annual_proof(acquired,defect):
    _,raw,supplement,_=acquired;proof=json.loads(project(raw,supplement)['source_evidence_json'])
    if defect=='amount':proof['inline_filing_supplement']['native_annual_history']['annual_revenue']+=1
    elif defect=='missing':proof['inline_filing_supplement'].pop('native_annual_history')
    elif defect=='fingerprint':proof['inline_filing_supplement']['native_annual_history']['fingerprint']='0'*64
    elif defect=='source_point':
        point=next(p for p in proof['current']['revenue']['inputs'] if 'native_annual_anchor' in p);point['value']+=1
    assert not annual.cached_valid(proof) and not native.cached_currency_valid(proof)


def test_existing_history_conflict_cannot_be_overridden(acquired):
    _,raw,supplement,_=deepcopy(acquired);facts=json.loads(raw['facts'])
    facts['facts']['ifrs-full']['Revenue']['units']['EUR'].append(dict(start='2024-01-01',end='2024-12-31',val=1,
        filed='2026-09-30',form='6-K',accn='0001213900-26-105353'))
    raw['facts']=json.dumps(facts).encode()
    with pytest.raises(ValueError,match='native_annual_existing_observation_conflict'):project(raw,supplement)


def test_preparation_version_rotates_known_affected_symbol_only():
    from app.services import sec_fundamentals_preparation as prep
    assert annual.VERSION in prep.version_for_symbol('ADSE')
    assert annual.VERSION not in prep.version_for_symbol('ADAG')


@pytest.mark.parametrize('metric',['revenue_growth','operating_margin','current_ratio'])
def test_supported_alert_recovery_repeat_crossing_and_rollback(acquired,metric,monkeypatch):
    import test_sec_tagged_foreign_consumers as consumers
    _,raw,supplement,_=acquired;value=project(raw,supplement)
    metrics={k:value[k] for k in json.loads(value['source_evidence_json'])['calculations']}
    monkeypatch.setattr(consumers,'originals',lambda symbol:(raw['facts'],raw['company'],CIK,{}))
    monkeypatch.setattr(consumers,'EXPECTED',{'ADSE':{'metrics':metrics}})
    monkeypatch.setattr(consumers,'NOW',NOW)
    monkeypatch.setattr(inline,'acquire',lambda *a,**k:deepcopy(supplement))
    consumers.test_original_source_score_and_alert_recovery_repeat_crossing_rollback('ADSE',metric,monkeypatch)


def test_preparation_persists_complete_evidence_and_repeats_without_source_requests(acquired,monkeypatch):
    from sqlalchemy import create_engine,select,func
    from sqlalchemy.orm import sessionmaker
    from app.db import Base
    from app.models import Event,EmailDelivery,FundamentalsCache
    from app.services import sec_fundamentals_preparation as prep
    manifest,raw,supplement,_=acquired;urls={m['url']:raw[k] for k,m in manifest.items()};calls=[]
    class Client:
        issuer_document_urls=frozenset()
        def get(self,url):calls.append(url);return urls[url]
    class Clock(datetime):
        @classmethod
        def now(cls,tz=None):return NOW
    engine=create_engine('sqlite://');Base.metadata.create_all(engine);factory=sessionmaker(bind=engine)
    monkeypatch.setattr(socket.socket,'connect',lambda *a,**k:pytest.fail('Unexpected network'))
    monkeypatch.setenv('SEC_FUNDAMENTALS_WARMING_ENABLED','1')
    monkeypatch.setenv('SEC_FINANCIAL_DISCOVERY_READ_ENABLED','0')
    monkeypatch.setattr(prep,'SessionLocal',factory)
    monkeypatch.setattr(prep,'datetime',Clock)
    monkeypatch.setattr(prep,'DirectSourceClient',Client)
    monkeypatch.setattr(prep.sec_directory,'directory',lambda:{'ADSE':{'cik':CIK}})
    payload=prep.prepare('ADSE');value=project(raw,supplement)
    assert payload['values']=={k:value[k] for k in json.loads(value['source_evidence_json'])['calculations']}
    assert payload['calculation_evidence']==json.loads(value['source_evidence_json'])
    assert calls==[manifest[k]['url'] for k in ('company','facts','cover','current','annual')]
    assert prep.prepare('ADSE')==payload and len(calls)==5
    with factory() as db:
        assert all(db.scalar(select(func.count()).select_from(m))==0 for m in (Event,EmailDelivery,FundamentalsCache))
    engine.dispose()


def test_optional_annual_transport_failure_preserves_current_statement(acquired,monkeypatch):
    from app.clients.direct_sources import DirectSourceError
    _,raw,supplement,_=acquired
    baseline=deepcopy(supplement);baseline.pop('annual_history');baseline['evidence'].pop('native_annual_history')
    def unavailable(*a,**k):raise DirectSourceError('Source transport failed')
    monkeypatch.setattr(annual,'acquire',unavailable)
    manifest,_=originals();held,calls=acquire(raw,manifest)
    value=project(raw,held)
    assert value['current_ratio']==pytest.approx(0.9362844043874885)
    assert not {'revenue_growth','net_margin','free_cash_flow'} & value.keys()
    assert 'annual_history' not in held
    assert calls==[manifest[k]['url'] for k in ('cover','current')]


@pytest.mark.parametrize('defect',['existing_zero','existing_conflict','comparative_conflict','future_amendment','not_interim'])
def test_annual_plan_does_not_replace_present_or_incomparable_evidence(acquired,defect):
    from datetime import date
    _,raw,current,_=deepcopy(acquired)
    current.pop('annual_history');current['evidence'].pop('native_annual_history')
    facts=json.loads(raw['facts']);company=json.loads(raw['company'])
    if defect in {'existing_zero','existing_conflict'}:
        points=facts['facts']['ifrs-full']['Revenue']['units']['EUR']
        row=dict(start='2025-01-01',end='2025-12-31',val=0,filed='2026-05-15',form='20-F',accn='0001213900-26-057891')
        points.append(row)
        if defect=='existing_conflict':points.append({**row,'val':1})
    elif defect=='comparative_conflict':
        for p in current['facts']['RevenueFromContractsWithCustomers']['units']['EUR']:
            if p.get('start')=='2025-01-01' and p['end']=='2025-06-30':p['val']+=1
    elif defect=='future_amendment':
        recent=company['filings']['recent'];i=recent['accessionNumber'].index('0001213900-26-057891')
        recent['filingDate'][i]='2026-10-01'
    else:current['evidence']['report_date']='2026-12-31'
    assert annual.plan(facts,company,current,available_by=date(2026,11,15)) is None
