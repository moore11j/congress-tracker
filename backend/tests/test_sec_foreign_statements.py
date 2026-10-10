from copy import deepcopy
from datetime import datetime, timezone
import json
import pytest
from sqlalchemy import create_engine, select, func
from sqlalchemy.orm import sessionmaker
from app.clients.direct_sources import DirectSourceError
from app.db import Base
from app.models import InsightsSnapshot, FundamentalsCache, Event
from app.services import sec_foreign_statements as foreign, sec_financial_statements as statements, sec_inline_financials as inline
from test_sec_inline_financials import document

NOW=datetime(2026,10,10,23,tzinfo=timezone.utc)


def sources(symbol='NVO'):
    basis=foreign.profile(symbol,foreign.PROFILES[symbol]['cik'])
    data={'cik':int(basis['cik']),'facts':{basis['namespace']:{}}}
    company={'cik':basis['cik'],'name':'Reviewed foreign issuer','tickers':[symbol],'filings':{'recent':{}}}
    values={'revenue':100,'net_income':20,'gross_profit':60,'operating_income':30,'operating_cash_flow':40,'capex':10}
    for metric,tags in basis['tags'].items():
        rows=[dict(start=f'{year}-01-01',end=f'{year}-12-31',val=value,
            accn=f'0000353278-{year+1-2000}-000012',form='20-F',filed=f'{year+1}-02-04')
            for year,value in [(2024,values[metric]*.8),(2025,values[metric])]]
        data['facts'][basis['namespace']][tags[0]]={'units':{basis['currency']:rows,'USD':[{**r,'val':999} for r in rows]}}
    return data,company,basis


def project(data,company,symbol='NVO',**kw):
    return statements.project(json.dumps(data).encode(),json.dumps(company).encode(),
        symbol=symbol,cik=company['cik'],observed_at=kw.get('now',NOW),filing_supplement=kw.get('supplement'))


@pytest.mark.parametrize('symbol,currency',[('NVO','DKK'),('BABA','CNY'),('TSM','TWD')])
def test_annual_currency_and_provenance_never_become_usd_ttm_or_adr_eps(symbol,currency):
    data,company,basis=sources(symbol);before=deepcopy(data)
    result=project(data,company,symbol)
    assert result['reportingCurrency']==currency
    assert result['annual'][-1]['revenue']==100
    assert result['annual'][-1]['freeCashFlow']==30
    assert result['annual'][-1]['grossMargin']==60
    assert result['annual'][-1]['currency']==currency and result['annual'][-1]['eps'] is None
    assert not result['quarterly'] and all(v is None for v in result['summary'].values())
    assert result['forecasts']['nextQuarter'] is None and result['status']=='partial'
    proof=result['sourceEvidence']['periods']['2025-12-31']['revenue']
    assert proof['unit']==currency and proof['value']==100 and proof['accession']
    assert data==before


@pytest.mark.parametrize('kind',['currency','conflict','form','today','unmatched_start','ambiguous_start'])
def test_bad_revenue_is_not_replaced_with_convenience_usd(kind):
    data,company,basis=sources()
    metric=data['facts']['ifrs-full']['Revenue']['units']
    row=metric['DKK'][-1]
    if kind=='currency':metric.pop('DKK')
    if kind=='conflict':metric['DKK'].append({**row,'val':101})
    if kind=='form':row['form']='6-K'
    if kind=='today':row['filed']='2026-10-10'
    if kind=='unmatched_start':row['start']='2025-04-01'
    if kind=='ambiguous_start':metric['DKK'].append({**row,'start':'2024-12-30'})
    assert all(r['date']!='2025-12-31' for r in project(data,company)['annual'])


def test_partial_restatement_does_not_mix_accessions():
    data,company,basis=sources()
    rows=data['facts']['ifrs-full']['Revenue']['units']['DKK']
    rows.append({**rows[-1],'filed':'2026-10-08','accn':'0000353278-26-000099','val':110})
    latest=project(data,company)['annual'][-1]
    assert latest['revenue']==110 and latest['netIncome'] is None and latest['freeCashFlow'] is None


@pytest.mark.parametrize('kind',['facts_cik','company_cik','ticker','duplicate_ticker','naive'])
def test_identity_and_observation_are_required(kind):
    data,company,basis=sources()
    now=NOW
    if kind=='facts_cik':data['cik']=1
    if kind=='company_cik':company['cik']='1'
    if kind=='ticker':company['tickers']=['OTHER']
    if kind=='duplicate_ticker':company['tickers']=['NVO','NVO']
    if kind=='naive':now=NOW.replace(tzinfo=None)
    with pytest.raises((DirectSourceError,ValueError)):project(data,company,now=now)


def foreign_document():
    return (document().replace(b'xmlns:us-gaap="http://fasb.org/us-gaap/2026"',
        b'xmlns:ifrs-full="https://xbrl.ifrs.org/taxonomy/2025-03-27/ifrs-full"')
        .replace(b'us-gaap:RegulatedAndUnregulatedOperatingRevenue',b'ifrs-full:Revenue')
        .replace(b'iso4217:USD',b'iso4217:DKK').replace(b'2026-01-01',b'2025-01-01')
        .replace(b'2026-06-30',b'2025-12-31').replace(b'>10-Q<',b'>20-F<')
        .replace(b'>1<',b'>353278<'))


def parse(raw):
    return inline.parse(raw,cik='0000353278',accession='0000353278-26-000012',filed='2026-02-04',
        report_date='2025-12-31',form='20-F',url='https://www.sec.gov/Archives/edgar/data/353278/report.htm',
        basis=foreign.profile('NVO','353278'))


def test_foreign_inline_currency_namespace_and_identity():
    r=parse(foreign_document())
    assert r['facts']['Revenue']['units']['DKK'][0]['val']==70
    assert r['evidence']['namespace']=='ifrs-full'
    for old,new in [(b'iso4217:DKK',b'iso4217:USD'),(b'xbrl.ifrs.org',b'evil.example'),
        (b'>353278<',b'>1<'),(b'>20-F<',b'>10-K<')]:
        with pytest.raises(inline.InlineFactsError):parse(foreign_document().replace(old,new))


def test_bounded_original_recovery_then_cached_repeat_has_no_canonical_writes(monkeypatch):
    from app.services import sec_directory
    data,company,basis=sources()
    for content in data['facts']['ifrs-full'].values():
        content['units']['DKK']=content['units']['DKK'][:1]
    company['filings']['recent']=dict(accessionNumber=['0000353278-26-000012'],form=['20-F'],
        filingDate=['2026-02-04'],reportDate=['2025-12-31'],primaryDocument=['report.htm'])
    engine=create_engine('sqlite:///:memory:');Base.metadata.create_all(engine)
    factory=sessionmaker(bind=engine);monkeypatch.setattr(statements,'SessionLocal',factory)
    monkeypatch.setattr(sec_directory,'directory',lambda:{'NVO':{'cik':basis['cik']}})
    reads=[]
    def get(self,url):
        reads.append(url)
        if '/Archives/' in url:return foreign_document()
        return json.dumps(data if '/companyfacts/' in url else company).encode()
    monkeypatch.setattr(statements.DirectSourceClient,'get',get)
    first=statements.prepared('NVO');assert first['annual'][-1]['revenue']==70 and len(reads)==3
    with factory() as db:
        before=[(r.kind,r.payload_json,r.fetched_at) for r in db.scalars(select(InsightsSnapshot))]
    assert statements.prepared('NVO')==first and len(reads)==3
    with factory() as db:
        assert before==[(r.kind,r.payload_json,r.fetched_at) for r in db.scalars(select(InsightsSnapshot))]
        assert db.scalar(select(func.count()).select_from(FundamentalsCache))==0
        assert db.scalar(select(func.count()).select_from(Event))==0
    engine.dispose()
