"""Complete SEC originals, independent expected values, and field-level holds."""
from pathlib import Path
from datetime import datetime, timezone
from copy import deepcopy
import gzip, hashlib, json
import pytest
from app.services import sec_fundamentals as sec
from app.services.sec_nonrevenue_financials import VERSION, PROFILES
from app.services.sec_fundamentals_preparation import version_for_symbol

ROOT = Path(__file__).parent/'fixtures/sec_nonrevenue'
NOW = datetime(2026,10,17,1,25,tzinfo=timezone.utc)
EXPECTED = {'ANIX': {'current_ratio': 14911000/1745000, 'roe': -66.83834417047576},
            'BCDA': {'current_ratio': 4298000/2055000, 'free_cash_flow': -7572000.0}}

def sources(symbol):
    manifest=json.loads((ROOT/'sources.json').read_text())
    result=[]
    for kind in ['facts','company']:
        name=f'{symbol}-{kind}.json';raw=gzip.decompress((ROOT/(name+'.gz')).read_bytes())
        assert hashlib.sha256(raw).hexdigest()==manifest[name]['sha256']
        result.append(json.loads(raw))
    return result

def project(symbol,facts=None,company=None,now=NOW):
    original=sources(symbol)
    return sec.project_fundamentals(json.dumps(facts or original[0]).encode(),
        json.dumps(company or original[1]).encode(),symbol=symbol,cik=PROFILES[symbol],observed_at=now)

@pytest.mark.parametrize('symbol',list(PROFILES))
def test_complete_originals_recover_only_independent_metrics(symbol):
    result=project(symbol);evidence=json.loads(result['source_evidence_json'])
    for name,value in EXPECTED[symbol].items():assert result[name]==pytest.approx(value)
    assert set(evidence['calculations'])==set(EXPECTED[symbol])
    assert all(k not in result for k in ['gross_margin','operating_margin','net_margin','revenue_growth','fcf_margin','fcf_growth','net_debt_to_ebitda'])
    assert evidence['current']['revenue'] is None and evidence['financial_status']=='partial'
    assert VERSION in version_for_symbol(symbol)
    assert project(symbol)==result

@pytest.mark.parametrize('symbol',list(PROFILES))
@pytest.mark.parametrize('defect',['identity','no_filings','future_report','stale','ambiguous'])
def test_wrong_stale_or_ambiguous_filing_is_held(symbol,defect):
    facts,company=sources(symbol);recent=company['filings']['recent']
    if defect=='identity':company['cik']=123
    elif defect=='no_filings':company['filings']['recent']={}
    elif defect=='future_report':
        # A newer valid filing has no matching fact evidence. No old-ratio reuse.
        for key,value in {'form':'10-Q','reportDate':'2026-09-30','filingDate':'2026-10-14','accessionNumber':'0000000000-26-000001'}.items():recent[key].insert(0,value)
    elif defect=='ambiguous':
        indexes=[i for i,f in enumerate(recent['form']) if f in {'10-Q','10-K'}]
        i=max(indexes,key=lambda i:(recent['reportDate'][i],recent['filingDate'][i]))
        for key in ['form','reportDate','filingDate','accessionNumber']:recent[key].insert(0,recent[key][i])
        recent['accessionNumber'][0]='0000000000-26-000001'
    with pytest.raises(sec.SecFundamentalsError):
        project(symbol,facts,company,now=NOW.replace(year=2027) if defect=='stale' else NOW)

@pytest.mark.parametrize('symbol',list(PROFILES))
@pytest.mark.parametrize('defect',['wrong_currency','conflicting_amount','nonfinite','wrong_accession'])
def test_invalid_balance_evidence_does_not_remove_other_verified_fields(symbol,defect):
    facts,company=sources(symbol);concept=facts['facts']['us-gaap']['AssetsCurrent']
    rows=concept['units']['USD'];latest=max(rows,key=lambda r:(r['end'],r['filed']))
    if defect=='wrong_currency':concept['units']['EUR']=concept['units'].pop('USD')
    elif defect=='conflicting_amount':rows.append({**latest,'val':latest['val']+1})
    elif defect=='nonfinite':latest['val']=float('nan')
    else:latest['accn']='0000000000-26-000001'
    result=project(symbol,facts,company)
    assert 'current_ratio' not in result
    for name,value in EXPECTED[symbol].items():
        if name!='current_ratio':assert result[name]==pytest.approx(value)

def test_missing_capex_is_not_assumed_zero():
    facts,company=sources('BCDA');facts['facts']['us-gaap'].pop('PaymentsToAcquirePropertyPlantAndEquipment')
    result=project('BCDA',facts,company)
    assert 'free_cash_flow' not in result and result['current_ratio']==pytest.approx(EXPECTED['BCDA']['current_ratio'])

def test_negative_equity_does_not_create_roe():
    facts,company=sources('ANIX');rows=facts['facts']['us-gaap']['StockholdersEquity']['units']['USD']
    max(rows,key=lambda r:(r['end'],r['filed']))['val']=-1
    result=project('ANIX',facts,company)
    assert 'roe' not in result and result['current_ratio']==pytest.approx(EXPECTED['ANIX']['current_ratio'])

def test_unreviewed_issuer_cannot_use_partial_policy():
    facts,company=sources('ANIX');company['tickers']=['OTHER']
    with pytest.raises(sec.SecFundamentalsCoverageError):
        sec.project_fundamentals(json.dumps(facts).encode(),json.dumps(company).encode(),symbol='OTHER',cik=PROFILES['ANIX'],observed_at=NOW)


@pytest.mark.parametrize('symbol',list(PROFILES))
def test_actual_acquisition_preparation_score_and_alert_consumers(symbol,monkeypatch):
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
    assert proof['inline_filing_supplement']['sha256']==manifest[symbol+'-current.htm']['sha256']
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
        threshold=new['current_ratio']-1
        rule=WatchlistAlertRule(user_id=user.id,watchlist_id=watch.id,name='Current ratio',enabled=True,scope_type='any_watchlist_ticker',match_type='all',delivery='both',conditions_json=json.dumps(validate_conditions([dict(metric='current_ratio',operator='gt',comparison_type='value',comparison_value=threshold)])))
        row=FundamentalsCache(symbol=symbol,provider='sec_edgar',status='ok',fetched_at=NOW,source_evidence_json=json.dumps({'financial_status':'unavailable','calculations':{}}))
        db.add_all([rule,row]);db.commit();monkeypatch.setenv('FUNDAMENTALS_PROVIDER','sec_edgar')
        def run(minutes):
            result=evaluate_watchlist_custom_alerts(db,user_id=user.id,watchlist_id=watch.id,now=NOW+timedelta(minutes=minutes));db.commit();return result
        assert run(0)['triggered']==0
        for key,value in payload['values'].items():setattr(row,key,value)
        row.source_evidence_json=new['source_evidence_json']
        assert run(5)['triggered']==0 and run(10)['triggered']==0
        row.current_ratio=threshold-1;assert run(15)['triggered']==0
        row.current_ratio=new['current_ratio'];assert run(20)['triggered']==1 and run(25)['triggered']==0
        row.current_ratio=None;row.source_evidence_json=json.dumps({'financial_status':'unavailable','calculations':{}})
        assert run(30)['triggered']==0
        assert db.scalar(select(func.count()).select_from(MonitoringAlert))==1
        assert db.scalar(select(func.count()).select_from(EmailDelivery))==0
    engine.dispose()


@pytest.mark.parametrize('prefix',['xsi','xs','schema'])
@pytest.mark.parametrize('boolean',['true','1'])
def test_explicit_nil_is_missing_not_zero_and_withdraws_older_fact(prefix,boolean):
    from test_sec_inline_financials import document,fact,parse,ACC
    from app.services import sec_inline_financials as inline
    raw=document(fact('',tag='NetIncomeLoss',attrs=f'{prefix}:nil="{boolean}"'))
    raw=raw.replace(b'<html ',f'<html xmlns:{prefix}="http://www.w3.org/2001/XMLSchema-instance" '.encode(),1)
    parsed=parse(raw)
    assert 'NetIncomeLoss' not in parsed['facts']
    assert parsed['evidence']['nil_facts']==[{'tag':'NetIncomeLoss','unit':'USD','start':'2026-01-01','end':'2026-06-30'}]
    base={'cik':1,'facts':{'us-gaap':{'NetIncomeLoss':{'units':{'USD':[
        {'start':'2026-01-01','end':'2026-06-30','val':123,'filed':'2026-07-20','accn':'older'},
        {'start':'2026-01-01','end':'2026-06-30','val':456,'filed':'2026-09-20','accn':'newer'}]}}}}}
    original=deepcopy(base)
    merged=inline.merge(base,parsed,available_by=NOW.date())
    assert base==original
    assert [r['val'] for r in merged['facts']['us-gaap']['NetIncomeLoss']['units']['USD']]==[456]
    base['facts']['us-gaap']['NetIncomeLoss']['units']['USD'][0]['accn']=ACC
    with pytest.raises(inline.InlineFactsError,match='nil_companyfacts_conflict'):
        inline.merge(base,parsed,available_by=NOW.date())

@pytest.mark.parametrize('mode',['wrong_namespace','invalid_boolean','content','numeric_before','numeric_after'])
def test_nil_malformed_or_conflicting_source_is_held(mode):
    from test_sec_inline_financials import document,fact,parse
    from app.services import sec_inline_financials as inline
    nil=fact('1' if mode=='content' else '',tag='NetIncomeLoss',attrs='xs:nil="maybe"' if mode=='invalid_boolean' else 'xs:nil="true"')
    numeric=fact('123',tag='NetIncomeLoss')
    extra=(numeric+nil if mode=='numeric_before' else nil+numeric if mode=='numeric_after' else nil)
    raw=document(extra).replace(b'<html ',b'<html xmlns:xs="http://www.w3.org/2001/XMLSchema-instance" ',1)
    if mode=='wrong_namespace':raw=raw.replace(b'http://www.w3.org/2001/XMLSchema-instance',b'https://example.test/spoof')
    with pytest.raises(inline.InlineFactsError):parse(raw)


@pytest.mark.parametrize('mode',['wrong_namespace','content','numeric_before','numeric_after'])
def test_optional_nil_errors_hold_only_the_optional_fact(mode):
    from test_sec_inline_financials import document,fact,parse,TAG
    nil=fact('1' if mode=='content' else '',tag='ProfitLoss',attrs='xs:nil="true"')
    numeric=fact('123',tag='ProfitLoss')
    extra=(numeric+nil if mode=='numeric_before' else nil+numeric if mode=='numeric_after' else nil)
    raw=document(extra).replace(b'<html ',b'<html xmlns:xs="http://www.w3.org/2001/XMLSchema-instance" ',1)
    if mode=='wrong_namespace':raw=raw.replace(b'http://www.w3.org/2001/XMLSchema-instance',b'https://example.test/spoof')
    result=parse(raw)
    assert result['facts'][TAG]['units']['USD'][0]['val']==70
    assert 'ProfitLoss' not in result['facts']
    assert result['evidence']['auxiliary_holds'][0]['tag']=='ProfitLoss'
