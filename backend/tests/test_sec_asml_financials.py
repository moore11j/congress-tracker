from copy import deepcopy
from datetime import datetime,timezone
from pathlib import Path
import hashlib,json
import pytest
from app.services import sec_asml_financials as subject

ROOT=Path(__file__).parent/'fixtures/asml'
META=json.loads((ROOT/'metadata.json').read_text())
NOW=datetime(2026,10,14,16,tzinfo=timezone.utc)

def bundle(end='2026-06-28'):
    return {'asml_documents':{d:subject.parse((ROOT/(d+'-original.htm')).read_bytes(),end=d) for d in [end,subject.PAIRS[end]]}}

def project(data=None,company=None,facts=None,now=NOW):
    return subject.project(json.dumps(facts or META['facts']).encode(),json.dumps(company or META['company']).encode(),
        symbol='ASML',cik=subject.CIK,observed_at=now,supplement=data or bundle())

@pytest.mark.parametrize('day,end,revenue,income',[(14,'2026-06-28',35327500000,12513300000),(15,'2026-09-27',39473500000,15217700000)])
def test_originals_and_completed_filing_day(day,end,revenue,income):
    data=bundle(end);before=deepcopy(data);now=NOW.replace(day=day)
    result=project(data,now=now);assert result==project(data,now=now) and data==before
    e=json.loads(result['source_evidence_json'])
    assert e['current']['revenue']['value']==revenue and e['current']['operating_income']['value']==income
    assert e['reporting_currency']=='EUR' and str(result['period_date'])==end
    assert len(set(result)&{'gross_margin','operating_margin','net_margin','revenue_growth','operating_margin_expansion','roe','current_ratio','fcf_margin','fcf_growth'})==9
    assert not set(result)&{'free_cash_flow','eps_ttm','price_to_sales','trailing_pe','net_debt_to_ebitda'}
    assert subject.selected_period(META['company'],symbol='ASML',cik=subject.CIK,observed_at=now)==end

@pytest.mark.parametrize('stamp,end',[('2026-10-15T03:59:59+00:00','2026-06-28'),('2026-10-15T04:00:00+00:00','2026-09-27')])
def test_new_york_midnight_boundary(stamp,end):
    assert subject.selected_period(META['company'],symbol='ASML',cik=subject.CIK,observed_at=datetime.fromisoformat(stamp))==end

@pytest.mark.parametrize('end',list(subject.SOURCES))
def test_original_changes_hold(end):
    raw=(ROOT/(end+'-original.htm')).read_bytes()+b' '
    with pytest.raises(subject.AsmlFinancialError,match='original_changed'):subject.parse(raw,end=end)

@pytest.mark.parametrize('change',['currency','basis','header','missing','sign','cost','balance','cash_income','numeric','pages'])
def test_unsupported_or_corrupt_statement(monkeypatch,change):
    end='2026-06-28';raw=(ROOT/(end+'-original.htm')).read_bytes();s=raw.decode()
    changes={'currency':('&#8364;','USD'),'basis':('US GAAP','IFRS'),'header':('2025 2026 2025 2026','2026 2025 2026 2025'),
        'missing':('Income from operations','Missing income from operations'),
        'sign':('(4,291.1)','4,291.1'),'cost':('(4,291.1)','(4,390.1)'),
        'balance':('21,825.4','21,925.4'),'numeric':('18,093.4','NaN'),
        'pages':('<IMG src="financialstatementsusgaa003.jpg"','<span src="financialstatementsusgaa003.jpg"')}
    if change=='cash_income':
        start=s.index('Cash flows from operating activities Net income');s=s[:start]+s[start:].replace('5,674.3','5,674.4',1)
    else:
        old,new=changes[change];assert old in s;s=s.replace(old,new)
    raw=s.encode();monkeypatch.setitem(subject.SOURCES,end,dict(subject.SOURCES[end],sha256=hashlib.sha256(raw).hexdigest()))
    with pytest.raises(subject.AsmlFinancialError):subject.parse(raw,end=end)

@pytest.mark.parametrize('change',['cik','ticker','duplicate','future','amendment','annual_amendment','naive','stale','missing_annual','shape'])
def test_identity_and_unknown_reports_hold(change):
    company=deepcopy(META['company']);r=company['filings']['recent'];now=NOW
    if change=='cik':company['cik']=320193
    elif change=='ticker':company['tickers']=['ASMLF']
    elif change=='duplicate':
        i=r['accessionNumber'].index(subject.SOURCES['2026-06-28']['accession'])
        for a in r.values():a.append(a[i])
    elif change in ['future','amendment','annual_amendment']:
        row=['0000937966-26-999999','6-K','2026-10-12','2026-09-30','form6-kquarterlyfilings.htm']
        if change=='amendment':row[1]='6-K/A';row[3]='2026-06-28'
        if change=='annual_amendment':row[1]='20-F/A';row[3]='2025-12-31'
        for k,v in zip(r,row):r[k].append(v)
    elif change=='naive':now=NOW.replace(tzinfo=None)
    elif change=='stale':now=NOW.replace(year=2027,month=8)
    elif change=='missing_annual':
        i=r['accessionNumber'].index(subject.ANNUAL['accession'])
        for a in r.values():a.pop(i)
    elif change=='shape':r['form'].pop()
    with pytest.raises(subject.AsmlFinancialError):project(company=company,now=now)

@pytest.mark.parametrize('change',['currency','hash','accession','period','value','restatement','missing_document','identity','annual_missing','annual_conflict','annual_currency','annual_revision','annual_nonfinite','annual_large'])
def test_fact_bundles_and_annual_facts_are_bound(change):
    data=bundle();facts=deepcopy(META['facts']);r=data['asml_documents']['2026-06-28']['facts']['revenue'][0]
    if change in ['currency','hash','accession','period','value']:
        key={'currency':'unit','hash':'source_sha256','accession':'accession','period':'end','value':'value'}[change]
        r[key]=float('nan') if change=='value' else 'wrong'
    elif change=='restatement':r['value']+=1
    elif change=='missing_document':data['asml_documents'].pop('2025-06-29')
    elif change=='identity':facts['cik']=320193
    else:
        rows=facts['facts']['us-gaap']['RevenueFromContractWithCustomerExcludingAssessedTax']['units']['EUR']
        if change=='annual_missing':rows.pop()
        elif change=='annual_conflict':rows.append(dict(rows[0],val=rows[0]['val']+1))
        elif change=='annual_currency':facts['facts']['us-gaap']['RevenueFromContractWithCustomerExcludingAssessedTax']['units']={'USD':rows}
        elif change=='annual_revision':rows[0]['accn']='other'
        elif change=='annual_nonfinite':rows[0]['val']=float('nan')
        elif change=='annual_large':rows[0]['val']=1e25
    with pytest.raises(subject.AsmlFinancialError):project(data,facts=facts)

def test_acquisition_and_scoped_version(monkeypatch):
    from app.services import sec_inline_financials as inline,sec_fundamentals_preparation as prep,sec_fundamentals as sec
    calls=[]
    class Client:
        def get(self,url):
            calls.append(url);end=next(d for d,s in subject.SOURCES.items() if s['url']==url)
            return (ROOT/(end+'-original.htm')).read_bytes()
    raw=json.dumps(META['company']).encode();facts=json.dumps(META['facts']).encode()
    data=inline.acquire(facts,raw,symbol='ASML',cik=subject.CIK,observed_at=NOW,client=Client())
    assert len(calls)==2 and set(calls)=={subject.SOURCES[d]['url'] for d in ['2026-06-28','2025-06-29']}
    assert sec.project_fundamentals(facts,raw,symbol='ASML',cik=subject.CIK,observed_at=NOW,filing_supplement=data)==project(data)
    assert prep.version_for_symbol('ASML')==prep.VERSION+'_'+subject.VERSION
    assert prep.version_for_symbol('AAPL')==prep.VERSION
    calls.clear();bad=deepcopy(META['company']);bad['cik']=320193
    result=inline.acquire(facts,json.dumps(bad).encode(),symbol='ASML',cik=subject.CIK,observed_at=NOW,client=Client())
    assert result['asml_error'] and not calls
