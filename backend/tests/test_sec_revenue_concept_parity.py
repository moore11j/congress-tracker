"""Full original statements, single-basis calculations and consumer continuity."""
from pathlib import Path
from datetime import datetime, timezone, date
from copy import deepcopy
import gzip,hashlib,json
import pytest
from app.services import sec_fundamentals as sec, sec_revenue_concept_parity as parity
from app.services.sec_revenue_concept_parity import VERSION,PROFILES,TOTAL,CONTRACT
from app.services.sec_fundamentals_preparation import version_for_symbol

ROOT=Path(__file__).parent/'fixtures/sec_revenue_concepts'
NOW=datetime(2026,10,17,1,45,tzinfo=timezone.utc)
EXPECTED={'AEVA':{'gross_margin':27.2247430317622,'operating_margin':-610.9454579127697,
    'net_margin':-150.1157514584684,'revenue_growth':56.224231464737784,
    'operating_margin_expansion':383.31132328072033,'current_ratio':7.845780533836614,
    'free_cash_flow':-116670000.0,'fcf_margin':-540.1889063802204},
    'ALLE':{'operating_margin':20.832458901713885,'revenue_growth':10.625290202754996,
    'operating_margin_expansion':-0.2091755202022263,'current_ratio':1.9297568506368197}}
REVENUE={'AEVA':18079000+12398000-8879000,'ALLE':4067300000+2185100000-1963900000}

def raw(name):
    data=gzip.decompress((ROOT/(name+'.gz')).read_bytes())
    assert hashlib.sha256(data).hexdigest()==json.loads((ROOT/'sources.json').read_text())[name]['sha256']
    return data

def sources(symbol):
    return json.loads(raw(symbol+'-facts.json')),json.loads(raw(symbol+'-company.json'))

def project(symbol,facts=None,company=None,now=NOW):
    original=sources(symbol)
    return sec.project_fundamentals(json.dumps(facts or original[0]).encode(),json.dumps(company or original[1]).encode(),symbol=symbol,cik=PROFILES[symbol],observed_at=now)

@pytest.mark.parametrize('symbol',list(PROFILES))
def test_complete_originals_and_independent_statement_revenue(symbol):
    from app.services import sec_inline_financials as inline
    manifest=json.loads((ROOT/'sources.json').read_text());parsed={}
    for form in ['10-Q','10-K']:
        name=symbol+'-'+form+'.htm';m=manifest[name]
        parsed[form]=inline.parse(raw(name),cik=m['cik'],accession=m['accession'],filed=m['filed_at'],report_date=m['report_date'],form=form,url=m['url'])
    quarterly=parsed['10-Q']['facts']
    totals={(p['start'],p['end']):p['val'] for p in quarterly[TOTAL]['units']['USD']}
    contracts={(p['start'],p['end']):p['val'] for p in quarterly[CONTRACT]['units']['USD']}
    assert totals==contracts and len(totals)==4
    annual=parsed['10-K']['facts'][CONTRACT]['units']['USD']
    assert annual==parity.reviews()[symbol]['annual_facts']
    annual_value=next(p['val'] for p in annual if p['start']=='2025-01-01' and p['end']=='2025-12-31')
    independent=annual_value+totals[('2026-01-01','2026-06-30')]-totals[('2025-01-01','2025-06-30')]
    result=project(symbol);e=json.loads(result['source_evidence_json'])
    assert e['current']['revenue']['value']==independent==REVENUE[symbol]
    assert {p['tag'] for p in e['current']['revenue']['inputs']}=={CONTRACT}
    assert e['current']['revenue']['inputs'][0]['revenue_selection']['basis_spliced'] is False
    for metric,value in EXPECTED[symbol].items():assert result[metric]==pytest.approx(value)
    assert set(e['calculations'])==set(EXPECTED[symbol])
    assert project(symbol)==result and VERSION in version_for_symbol(symbol)

@pytest.mark.parametrize('symbol',list(PROFILES))
@pytest.mark.parametrize('defect',['component_amount','component_currency','component_accession','component_conflict','annual_amount','annual_accession','annual_missing','prior_ytd_missing','new_report','ambiguous_report','identity','stale'])
def test_incomplete_or_changed_basis_cannot_become_total_revenue(symbol,defect):
    facts,company=sources(symbol);rows=facts['facts']['us-gaap'][CONTRACT]['units']['USD']
    latest=[r for r in rows if r.get('end')=='2026-06-30']
    if defect=='component_amount':
        for r in latest:r['val']+=1
    elif defect=='component_currency':facts['facts']['us-gaap'][CONTRACT]['units']['EUR']=facts['facts']['us-gaap'][CONTRACT]['units'].pop('USD')
    elif defect=='component_accession':
        for r in latest:r['accn']='0000000000-26-000001'
    elif defect=='component_conflict':rows.append({**latest[-1],'val':latest[-1]['val']+1})
    elif defect.startswith('annual_'):
        annual=[r for r in rows if r.get('start')=='2025-01-01' and r['end']=='2025-12-31']
        for r in annual:
            if defect=='annual_amount':r['val']+=1
            elif defect=='annual_accession':r['accn']='0000000000-26-000001'
            else:rows.remove(r)
    elif defect=='prior_ytd_missing':facts['facts']['us-gaap'][CONTRACT]['units']['USD']=[r for r in rows if not (r.get('start')=='2025-01-01' and r['end']=='2025-06-30')]
    elif defect in {'new_report','ambiguous_report'}:
        recent=company['filings']['recent'];i=next(i for i,f in enumerate(recent['form']) if f=='10-Q')
        for key in ['form','reportDate','filingDate','accessionNumber']:recent[key].insert(0,recent[key][i])
        recent['accessionNumber'][0]='0000000000-26-000001'
        if defect=='new_report':recent['reportDate'][0]='2026-09-30';recent['filingDate'][0]='2026-10-14'
    elif defect=='identity':company['cik']=123
    with pytest.raises(sec.SecFundamentalsError):project(symbol,facts,company,now=NOW.replace(year=2027) if defect=='stale' else NOW)

@pytest.mark.parametrize('symbol',list(PROFILES))
def test_complete_total_series_takes_precedence_and_is_never_spliced(symbol):
    facts,company=sources(symbol)
    source=facts['facts']['us-gaap'][CONTRACT]['units']['USD']
    facts['facts']['us-gaap'][TOTAL]['units']['USD']=deepcopy(source)
    result=project(symbol,facts,company);e=json.loads(result['source_evidence_json'])
    assert {p['tag'] for p in e['current']['revenue']['inputs']}=={TOTAL}
    assert all('revenue_selection' not in p for p in e['current']['revenue']['inputs'])

@pytest.mark.parametrize('symbol',list(PROFILES))
def test_nil_current_revenue_cannot_recover_through_another_concept(symbol):
    facts,company=sources(symbol)
    records={'revenue':sec._records(facts,'revenue',date(2026,10,15))}
    for tag in [TOTAL,CONTRACT]:
        supplement={'evidence':{'nil_facts':[{'tag':tag,'end':'2026-06-30'}]}}
        with pytest.raises(sec.SecFundamentalsCoverageError,match='nil_current_revenue'):
            parity.apply(records,facts,company,symbol=symbol,cik=PROFILES[symbol],available_by=date(2026,10,15),supplement=supplement)

def test_unreviewed_symbols_remain_unchanged():
    records={'revenue':{}}
    assert parity.apply(records,{}, {},symbol='OTHER',cik='0000000001',available_by=date(2026,10,15)) is records
    assert VERSION not in version_for_symbol('MSFT')


@pytest.mark.parametrize('symbol',list(PROFILES))
@pytest.mark.parametrize('metric',['revenue_growth','operating_margin'])
def test_actual_acquisition_preparation_score_and_alert_consumers(symbol,monkeypatch,metric):
    import socket,types
    from datetime import timedelta
    from unittest.mock import patch
    from sqlalchemy import create_engine,select,func
    from sqlalchemy.orm import sessionmaker
    from app.db import Base
    from app.models import (FundamentalsCache,Event,EmailDelivery,MonitoringAlert,
        Security,UserAccount,Watchlist,WatchlistItem,WatchlistAlertRule)
    from app.services import sec_fundamentals_preparation as prep,sec_inline_financials as inline
    from app.services.fundamentals_cache import fundamentals_summary_from_cache_row
    from app.services.confirmation_score import _fundamentals_context_source
    from app.services.custom_alert_rules import evaluate_watchlist_custom_alerts,validate_conditions
    monkeypatch.setattr(socket.socket,'connect',lambda *a,**k:pytest.fail('Unexpected network'))
    manifest=json.loads((ROOT/'sources.json').read_text());urls={}
    for name,meta in manifest.items():
        if name.startswith(symbol+'-'):
            raw=gzip.decompress((ROOT/(name+'.gz')).read_bytes())
            assert hashlib.sha256(raw).hexdigest()==meta['sha256'];urls[meta['url']]=raw
    calls=[]
    class Client:
        issuer_document_urls=frozenset()
        def get(self,url):calls.append(url);return urls[url]
    class Clock(datetime):
        @classmethod
        def now(cls,tz=None):return NOW
    facts=urls[manifest[symbol+'-facts.json']['url']];company=urls[manifest[symbol+'-company.json']['url']]
    supplement=inline.acquire(facts,company,symbol=symbol,cik=PROFILES[symbol],observed_at=NOW,client=Client(),include_debt_precision=True)
    new=sec.project_fundamentals(facts,company,symbol=symbol,cik=PROFILES[symbol],observed_at=NOW,filing_supplement=supplement)
    for name,value in EXPECTED[symbol].items():assert new[name]==pytest.approx(value)
    proof=json.loads(new['source_evidence_json'])
    assert proof['inline_filing_supplement']['sha256']==manifest[symbol+'-10-Q.htm']['sha256']
    engine=create_engine('sqlite://');Base.metadata.create_all(engine);factory=sessionmaker(bind=engine)
    monkeypatch.setenv('SEC_FUNDAMENTALS_WARMING_ENABLED','1')
    with patch.object(prep,'SessionLocal',factory),patch.object(prep,'datetime',Clock),patch.object(prep,'DirectSourceClient',Client),patch.object(prep.sec_directory,'directory',return_value={symbol:{'cik':PROFILES[symbol]}}):
        payload=prep.prepare(symbol)
        assert payload['status']=='partial' and payload['values']=={k:new[k] for k in prep.METRICS if k in new}
        request_count=len(calls)
        assert prep.prepare(symbol)==payload and len(calls)==request_count
        with factory() as db:
            assert db.scalar(select(func.count()).select_from(FundamentalsCache))==0
            assert db.scalar(select(func.count()).select_from(Event))==0
    summary=fundamentals_summary_from_cache_row(types.SimpleNamespace(**new),now=NOW)
    assert _fundamentals_context_source(summary)
    with factory() as db:
        user=UserAccount(email=symbol+'@example.test');security=Security(symbol=symbol,name=symbol,asset_class='equity')
        db.add_all([user,security]);db.flush()
        watch=Watchlist(name='Partial financials',owner_user_id=user.id);db.add(watch);db.flush()
        db.add(WatchlistItem(watchlist_id=watch.id,security_id=security.id,target_type='ticker'))
        threshold=new[metric]-1
        rule=WatchlistAlertRule(user_id=user.id,watchlist_id=watch.id,name='Current ratio',enabled=True,scope_type='any_watchlist_ticker',match_type='all',delivery='both',conditions_json=json.dumps(validate_conditions([dict(metric=metric,operator='gt',comparison_type='value',comparison_value=threshold)])))
        row=FundamentalsCache(symbol=symbol,provider='sec_edgar',status='ok',fetched_at=NOW,source_evidence_json=json.dumps({'financial_status':'unavailable','calculations':{}}))
        db.add_all([rule,row]);db.commit();monkeypatch.setenv('FUNDAMENTALS_PROVIDER','sec_edgar')
        def run(minutes):
            result=evaluate_watchlist_custom_alerts(db,user_id=user.id,watchlist_id=watch.id,now=NOW+timedelta(minutes=minutes));db.commit();return result
        assert run(0)['triggered']==0
        for key,value in payload['values'].items():setattr(row,key,value)
        row.source_evidence_json=new['source_evidence_json']
        assert run(5)['triggered']==0 and run(10)['triggered']==0
        setattr(row,metric,threshold-1);assert run(15)['triggered']==0
        setattr(row,metric,new[metric]);assert run(20)['triggered']==1 and run(25)['triggered']==0
        setattr(row,metric,None);row.source_evidence_json=json.dumps({'financial_status':'unavailable','calculations':{}})
        assert run(30)['triggered']==0
        assert db.scalar(select(func.count()).select_from(MonitoringAlert))==1
        assert db.scalar(select(func.count()).select_from(EmailDelivery))==0
    engine.dispose()



@pytest.mark.parametrize('symbol',list(PROFILES))
def test_next_quarter_with_matching_reported_totals_rolls_forward(symbol):
    facts,company=sources(symbol);gaap=facts['facts']['us-gaap'];recent=company['filings']['recent']
    current=next(r for r in gaap[CONTRACT]['units']['USD'] if r.get('start')=='2026-01-01' and r['end']=='2026-06-30')
    previous=next(r for r in gaap[CONTRACT]['units']['USD'] if r.get('start')=='2025-01-01' and r['end']=='2025-09-30')
    acc='0000000000-26-000123';filed='2026-10-14';end='2026-09-30'
    for tag in [TOTAL,CONTRACT]:
        gaap[tag]['units']['USD'].extend([{**current,'val':current['val']+1000000,'end':end,'filed':filed,'accn':acc},
            {**previous,'filed':filed,'accn':acc}])
    for key,value in {'form':'10-Q','reportDate':end,'filingDate':filed,'accessionNumber':acc}.items():recent[key].insert(0,value)
    result=project(symbol,facts,company);e=json.loads(result['source_evidence_json'])
    annual=next(p['val'] for p in parity.reviews()[symbol]['annual_facts'] if p['start']=='2025-01-01' and p['end']=='2025-12-31')
    assert e['current']['revenue']['value']==annual+current['val']+1000000-previous['val']
    assert str(result['period_date'])==end
    assert {p['tag'] for p in e['current']['revenue']['inputs']}=={CONTRACT}


@pytest.mark.parametrize('symbol',list(PROFILES))
def test_historical_nil_cannot_revive_a_reviewed_annual_fact(symbol):
    facts,company=sources(symbol)
    from app.services import sec_inline_financials as inline
    manifest=json.loads((ROOT/'sources.json').read_text());name=symbol+'-10-Q.htm';m=manifest[name]
    supplement=inline.parse(raw(name),cik=m['cik'],accession=m['accession'],filed=m['filed_at'],report_date=m['report_date'],form=m['form'],url=m['url'])
    supplement['evidence']['nil_facts']=[{'tag':CONTRACT,'unit':'USD','start':'2025-01-01','end':'2025-12-31'}]
    with pytest.raises(sec.SecFundamentalsCoverageError):
        sec.project_fundamentals(json.dumps(facts).encode(),json.dumps(company).encode(),symbol=symbol,cik=PROFILES[symbol],observed_at=NOW,filing_supplement=supplement)
