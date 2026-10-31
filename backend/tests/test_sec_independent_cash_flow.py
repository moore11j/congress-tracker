"""Independent cash-flow growth uses complete matched periods, never revenue proxies."""
from datetime import datetime, timezone
from pathlib import Path
import gzip, hashlib, json, socket
import pytest
from app.services import sec_fundamentals as sec
from test_sec_independent_financials import inputs, project

ROOT = Path(__file__).parent/'fixtures/sec_independent_cash_flow'


@pytest.mark.parametrize('defect', ['missing_prior_cash', 'missing_prior_capex', 'negative_prior_capex',
    'conflicting_prior_cash', 'misaligned_prior_capex', 'nonpositive_prior_fcf', 'zero_prior_fcf',
    'future_prior_cash', 'ambiguous_prior_end', 'nonfinite_prior_cash'])
def test_bad_prior_period_holds_growth_without_losing_current_metrics(defect):
    facts, company = inputs(); gaap = facts['facts']['us-gaap']
    cash = gaap['NetCashProvidedByUsedInOperatingActivities']['units']['USD']
    capex = gaap['PaymentsToAcquirePropertyPlantAndEquipment']['units']['USD']
    if defect == 'missing_prior_cash':cash.pop(0)
    elif defect == 'missing_prior_capex':capex.pop(0)
    elif defect == 'negative_prior_capex':capex[0]['val'] = -100
    elif defect == 'conflicting_prior_cash':cash.append({**cash[0], 'val': 999})
    elif defect == 'misaligned_prior_capex':capex[0]['start'] = '2024-02-01'
    elif defect == 'nonpositive_prior_fcf':cash[0]['val'] = -100
    elif defect == 'zero_prior_fcf':cash[0]['val'] = 2.5
    elif defect == 'future_prior_cash':cash[0]['filed'] = '2026-10-09'
    elif defect == 'ambiguous_prior_end':cash.append({**cash[3], 'start':'2024-07-02', 'end':'2025-07-01'})
    else:cash[0]['val'] = float('nan')
    result = project(facts, company)
    assert 'fcf_growth' not in result
    assert result['free_cash_flow'] == 32.5
    assert result['current_ratio'] == 2


def test_negative_current_cash_flow_is_retained_and_missing_capex_is_not_zero():
    facts, company = inputs(); gaap = facts['facts']['us-gaap']
    gaap['NetCashProvidedByUsedInOperatingActivities']['units']['USD'][-1]['val'] = -50
    result = project(facts, company)
    assert result['free_cash_flow'] == -38.5
    assert result['fcf_growth'] == pytest.approx((-38.5/27.5-1)*100)
    gaap.pop('PaymentsToAcquirePropertyPlantAndEquipment')
    result = project(facts, company)
    assert 'free_cash_flow' not in result and 'fcf_growth' not in result


@pytest.mark.parametrize('symbol,expected', [('CMI',((3621+1808-782-(1235+438-393))/(1487+782+575-(1208+393-409))-1)*100),
    ('FISV',((6062+2082-2313-(1763+956-814))/(6631+2313-2172-(1569+814-768))-1)*100)])
def test_complete_public_sources_repeat_and_preserve_cash_flow_provenance(symbol,expected,monkeypatch):
    monkeypatch.setattr(socket.socket,'connect',lambda *a,**k:pytest.fail('Unexpected network'))
    manifest = json.loads((ROOT/'sources.json').read_text())
    originals = []
    for kind in ('facts','company'):
        name = f'{symbol}-{kind}.json'; raw = gzip.decompress((ROOT/(name+'.gz')).read_bytes())
        assert hashlib.sha256(raw).hexdigest() == manifest[name]['sha256']; originals.append(raw)
    cik = str(json.loads(originals[1])['cik']).zfill(10)
    kwargs = dict(symbol=symbol,cik=cik,observed_at=datetime(2026,10,31,21,40,tzinfo=timezone.utc))
    result = sec.project_fundamentals(*originals,**kwargs)
    evidence = json.loads(result['source_evidence_json'])
    assert result['fcf_growth'] == pytest.approx(expected)
    assert evidence['methodology'] == 'sec_independent_financials_v3'
    assert evidence['current']['revenue'] is None
    assert evidence['facts']['sha256'] == manifest[f'{symbol}-facts.json']['sha256']
    assert evidence['company']['sha256'] == manifest[f'{symbol}-company.json']['sha256']
    for period in ('current','prior'):
        cash, capex = [evidence[period][k] for k in ('operating_cash_flow','capex')]
        assert cash['start'] == capex['start'] and cash['end'] == capex['end']
        assert all(r['accession'] and r['filed'] for fact in (cash,capex) for r in fact['inputs'])
    assert sec.project_fundamentals(*originals,**kwargs) == result



@pytest.mark.parametrize('symbol,expected', [('CMI',(3367/1652-1)*100),('FISV',(3926/5157-1)*100)])
def test_original_acquisition_preparation_and_cache_repeat(symbol,expected,monkeypatch):
    from sqlalchemy import create_engine,select,func
    from sqlalchemy.orm import sessionmaker
    from app.db import Base
    from app.models import Event,EmailDelivery,FundamentalsCache
    from app.services import sec_fundamentals_preparation as prep
    from app.services.fundamentals_cache import fundamentals_summary_from_cache_row
    from app.services.confirmation_score import _fundamentals_context_source
    monkeypatch.setattr(socket.socket,'connect',lambda *a,**k:pytest.fail('Unexpected network'))
    manifest=json.loads((ROOT/'sources.json').read_text());urls={}
    for name,meta in manifest.items():
        if name.startswith(symbol+'-'):
            raw=gzip.decompress((ROOT/(name+'.gz')).read_bytes())
            assert hashlib.sha256(raw).hexdigest()==meta['sha256'];urls[meta['url']]=raw
    cik=str(json.loads(gzip.decompress((ROOT/(symbol+'-company.json.gz')).read_bytes()))['cik']).zfill(10)
    calls=[]
    class Client:
        issuer_document_urls=frozenset()
        def get(self,url):calls.append(url);return urls[url]
    now=datetime(2026,10,31,21,40,tzinfo=timezone.utc)
    class Clock(datetime):
        @classmethod
        def now(cls,tz=None):return now
    engine=create_engine('sqlite://');Base.metadata.create_all(engine);factory=sessionmaker(bind=engine)
    monkeypatch.setenv('SEC_FUNDAMENTALS_WARMING_ENABLED','1')
    monkeypatch.setattr(prep,'SessionLocal',factory);monkeypatch.setattr(prep,'datetime',Clock)
    monkeypatch.setattr(prep,'DirectSourceClient',Client)
    monkeypatch.setattr(prep.sec_directory,'directory',lambda:{symbol:{'cik':cik}})
    payload=prep.prepare(symbol)
    assert payload['values']['fcf_growth']==pytest.approx(expected)
    assert payload['calculation_evidence']['methodology']=='sec_independent_financials_v3'
    assert payload['calculation_evidence']['inline_filing_supplement']['sha256']==manifest[symbol+'-current.htm']['sha256']
    requests=len(calls);assert requests==3
    assert prep.prepare(symbol)==payload and len(calls)==requests
    with factory() as db:
        assert all(db.scalar(select(func.count()).select_from(model))==0 for model in (Event,EmailDelivery,FundamentalsCache))
    row=FundamentalsCache(symbol=symbol,provider='sec_edgar',status='ok',fetched_at=now,
        source_evidence_json=json.dumps(payload['calculation_evidence']),**payload['values'])
    summary=fundamentals_summary_from_cache_row(row,now=now)
    before=_fundamentals_context_source(summary)
    row.fcf_growth=None
    assert _fundamentals_context_source(fundamentals_summary_from_cache_row(row,now=now))==before
    engine.dispose()
