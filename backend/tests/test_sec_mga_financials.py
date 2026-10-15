from copy import deepcopy
from datetime import datetime, timezone, timedelta
from html import escape
from pathlib import Path
import hashlib
import json
import pytest
from app.services import sec_mga_financials as mga

FIXTURE=json.loads((Path(__file__).parent/'fixtures/mga_reviewed_tables.json').read_text())
NOW=datetime(2026,10,15,18,tzinfo=timezone.utc)


def company():
    rows=[(s['accession'],s['form'],s['filed'],s['report'],s['cover']) for s in mga.SOURCES.values()]
    return {'cik':749098,'name':'MAGNA INTERNATIONAL INC','tickers':['MGA'],'exchanges':['NYSE'],
        'filings':{'recent':dict(zip(('accessionNumber','form','filingDate','reportDate','primaryDocument'),map(list,zip(*rows))))}}


def render(key,tables=None):
    if tables is None:tables=FIXTURE[key]['tables']
    return ('<html><body>'+''.join('<p>'+escape(t['title'])+'</p><table>'+''.join('<tr>'+''.join('<td>'+escape(c)+'</td>' for c in r)+'</tr>' for r in t['rows'])+'</table>' for t in tables)+'</body></html>').encode()


def trust(monkeypatch,key,raw):
    monkeypatch.setitem(mga.SOURCES,key,dict(mga.SOURCES[key],sha256=hashlib.sha256(raw).hexdigest()))


def bundle(monkeypatch):
    originals={k:render(k) for k in mga.SOURCES}
    for k,raw in originals.items():trust(monkeypatch,k,raw)
    return {'mga_originals':originals}


def project(data,**kwargs):
    args=dict(symbol='MGA',cik=mga.CIK,observed_at=NOW,supplement=data);args.update(kwargs)
    return mga.project(b'{"cik":749098}',json.dumps(company()).encode(),**args)


def test_full_statements_reconcile_current_prior_and_exact_repeat(monkeypatch):
    for key,s in mga.SOURCES.items():assert s['sha256']==FIXTURE[key]['original_sha256']
    data=bundle(monkeypatch);before=deepcopy(data);r=project(data)
    assert r==project(data) and data==before
    e=json.loads(r['source_evidence_json'])
    assert e['current']['revenue']['value']==42_671_000_000
    assert e['prior']['revenue']['value']==41_608_000_000
    assert e['current']['net_income']['value']==761_000_000
    assert e['current']['consolidated_income']['value']==814_000_000
    assert e['current']['operating_cash_flow']['value']==4_525_000_000
    assert e['current']['capex_outflow']['value']==-1_287_000_000
    assert r['free_cash_flow']==3_238_000_000
    assert r['revenue_growth']==pytest.approx(2.5547971543933956)
    assert r['roe']==pytest.approx(6.276806334543054)
    assert r['net_margin']==pytest.approx(1.7834126221555624)
    assert r['current_ratio']==pytest.approx(1.2152175742782594)
    assert not set(r)&{'operating_margin','operating_margin_expansion','gross_margin','eps_ttm','eps_growth','net_debt_to_ebitda'}
    assert len(e['sources'])==3 and e['reporting_currency']=='USD'
    for group in ('current','prior'):
        for point in e[group].values():
            assert len(point['inputs'])==3
            assert all(i['unit']=='USD' and i['source_sha256'] for i in point['inputs'])


@pytest.mark.parametrize('key',list(mga.SOURCES))
def test_unreviewed_original_fails_hash(key):
    with pytest.raises(mga.MgaFinancialError,match='original_changed'):mga.parse(render(key),source_key=key)


@pytest.mark.parametrize('change',['currency','issuer','consolidation','date','year','columns','missing','duplicate','note_as_value','cash_mismatch','income_mismatch','equity_mismatch','extra_total','duplicate_statement'])
def test_malformed_or_incompatible_tables_fail_even_after_hash_review(monkeypatch,change):
    tables=deepcopy(FIXTURE['current']['tables']);income,balance,cash=tables
    if change=='currency':income['title']=income['title'].replace('U.S.','Canadian')
    elif change=='issuer':income['title']=income['title'].replace('MAGNA','OTHER')
    elif change=='consolidation':income['title']=income['title'].replace('CONSOLIDATED','PARENT')
    elif change=='date':
        for row in income['rows']:
            row[:]=[c.replace('June 30,','September 30,') for c in row]
    elif change=='year':
        for row in income['rows']:row[:]=['2027' if c=='2026' else c for c in row]
    elif change=='columns':
        row=next(r for r in income['rows'] if 'Note' in r);ids=[i for i,c in enumerate(row) if c in ('2026','2025')];row[ids[0]],row[ids[1]]=row[ids[1]],row[ids[0]]
    elif change=='missing':income['rows'][:]=[r for r in income['rows'] if not r or r[0]!='Sales']
    elif change=='duplicate':income['rows'].append(deepcopy(next(r for r in income['rows'] if r[0]=='Sales')))
    elif change=='note_as_value':
        row=next(r for r in income['rows'] if r[0]=='Sales');row[row.index('10,980')]='17'
    elif change=='cash_mismatch':
        row=next(r for r in cash['rows'] if r[0]=='Cash provided from operating activities');row[row.index('954')]='955'
    elif change=='income_mismatch':
        row=next(r for r in income['rows'] if r[0]=='Cost of goods sold');row[row.index('9,368')]='9,369'
    elif change=='equity_mismatch':
        row=next(r for r in balance['rows'] if r[0]=='Retained earnings');row[row.index('9,183')]='9,184'
    elif change=='extra_total':balance['rows'].append(deepcopy(next(r for r in balance['rows'] if not r[0] and '14,438' in r)))
    elif change=='duplicate_statement':tables.append(deepcopy(income))
    raw=render('current',tables);trust(monkeypatch,'current',raw)
    with pytest.raises(mga.MgaFinancialError):mga.parse(raw,source_key='current')


@pytest.mark.parametrize('cells,n', [(['1','2','3'],2),(['NaN'],1),(['1e9'],1),(['-'],1),(['12,34'],1),(['(10'],1),(['1.25'],1),(['999999999999'],1)])
def test_ambiguous_number_is_never_zero(cells,n):
    with pytest.raises(mga.MgaFinancialError):mga._amounts(cells,n)


def test_negative_and_explicit_zero():
    assert mga._amounts(['$','0','(','1,000',')'],2)==[0,-1_000_000_000]


@pytest.mark.parametrize('change',['wrong_cik','wrong_ticker','missing_source','new_report','amendment','filing_today','stale','naive','duplicate_source','bad_arrays'])
def test_directory_and_point_in_time_holds_before_transport(monkeypatch,change):
    from app.services import sec_inline_financials as inline
    c=company();recent=c['filings']['recent'];now=NOW
    if change=='wrong_cik':c['cik']=320193
    elif change=='wrong_ticker':c['tickers']=['OTHER']
    elif change=='missing_source':
        for a in recent.values():a.pop()
    elif change=='duplicate_source':
        for a in recent.values():a.append(a[0])
    elif change=='bad_arrays':recent['form'].pop()
    elif change in ('new_report','amendment'):
        row=['0000749098-26-999999','6-K' if change=='new_report' else '6-K/A','2026-10-14','2026-09-30','new.htm']
        for key,v in zip(recent,row):recent[key].append(v)
    elif change=='filing_today':now=NOW.replace(month=7,day=31)
    elif change=='stale':now=NOW.replace(year=2027)
    elif change=='naive':now=NOW.replace(tzinfo=None)
    class NoFetch:
        def get(self,url):raise AssertionError('Invalid directory must not fetch')
    r=inline.acquire(b'{"cik":749098}',json.dumps(c).encode(),symbol='MGA',cik=mga.CIK,observed_at=now,client=NoFetch())
    assert r['mga_error']


def test_exact_three_fetches_project_dispatch_and_selective_version(monkeypatch):
    from app.services import sec_inline_financials as inline,sec_fundamentals as sec,sec_fundamentals_preparation as prep
    data=bundle(monkeypatch);calls=[];urls={mga.url_for(s):data['mga_originals'][k] for k,s in mga.SOURCES.items()}
    class Client:
        def get(self,url):calls.append(url);return urls[url]
    result=inline.acquire(b'{"cik":749098}',json.dumps(company()).encode(),symbol='MGA',cik=mga.CIK,observed_at=NOW,client=Client())
    assert result==data and len(calls)==3
    assert sec.project_fundamentals(b'{"cik":749098}',json.dumps(company()).encode(),symbol='MGA',cik=mga.CIK,observed_at=NOW,filing_supplement=result)==project(data)
    assert prep.version_for_symbol('MGA')==prep.VERSION+'_'+mga.VERSION
    assert prep.version_for_symbol('AAPL')==prep.VERSION


def test_transport_cooldown_is_not_swallowed():
    from app.services import sec_inline_financials as inline
    from app.clients.direct_sources import DirectSourceError
    class Client:
        def get(self,url):raise DirectSourceError('Source cooldown')
    with pytest.raises(DirectSourceError,match='cooldown'):
        inline.acquire(b'{"cik":749098}',json.dumps(company()).encode(),symbol='MGA',cik=mga.CIK,observed_at=NOW,client=Client())


def test_missing_or_tampered_bundle_and_wrong_facts_identity(monkeypatch):
    data=bundle(monkeypatch)
    broken=deepcopy(data);broken['mga_originals']['current']+=b' '
    with pytest.raises(mga.MgaFinancialError,match='original_changed'):project(broken)
    broken=deepcopy(data);broken['mga_originals'].pop('annual')
    with pytest.raises(mga.MgaFinancialError,match='incomplete'):project(broken)
    with pytest.raises(mga.MgaFinancialError,match='companyfacts_identity'):
        mga.project(b'{"cik":1}',json.dumps(company()).encode(),symbol='MGA',cik=mga.CIK,observed_at=NOW,supplement=data)


from test_sec_fundamentals_warming import factory

def test_actual_preparation_persistence_and_repeat_without_canonical_writes(factory,monkeypatch):
    from app.services import sec_fundamentals_preparation as prep,sec_directory
    from app.clients.direct_sources import DirectSourceClient
    from app.models import InsightsSnapshot,FundamentalsCache,Event
    from sqlalchemy import select,func
    data=bundle(monkeypatch);calls=[]
    class Clock(datetime):
        @classmethod
        def now(cls,tz=None):return NOW
    monkeypatch.setattr(prep,'datetime',Clock)
    monkeypatch.setattr(sec_directory,'directory',lambda:{'MGA':{'cik':mga.CIK}})
    urls={mga.url_for(s):data['mga_originals'][k] for k,s in mga.SOURCES.items()}
    def fetch(self,url):
        calls.append(url)
        return urls[url] if '/Archives/' in url else json.dumps({'cik':749098} if 'companyfacts' in url else company()).encode()
    monkeypatch.setattr(DirectSourceClient,'get',fetch)
    first=prep.prepare('MGA')
    assert first['status']=='partial' and len(first['values'])==7 and len(calls)==5
    assert first['values']=={k:project(data)[k] for k in first['values']}
    with factory() as db:before=db.get(InsightsSnapshot,'sec-current-fundamentals:MGA:v1').payload_json
    assert prep.prepare('MGA')==first and len(calls)==5
    with factory() as db:
        assert db.get(InsightsSnapshot,'sec-current-fundamentals:MGA:v1').payload_json==before
        assert db.scalar(select(func.count()).select_from(FundamentalsCache))==0
        assert db.scalar(select(func.count()).select_from(Event))==0


def test_same_day_companion_is_reviewed_but_other_filings_are_held():
    c=company();r=c['filings']['recent']
    for key,value in zip(r,mga.COMPANION):r[key].append(value)
    mga.validate_company(c,symbol='MGA',cik=mga.CIK,observed_at=NOW)
    r['accessionNumber'][-1]='0000749098-26-888888'
    with pytest.raises(mga.MgaFinancialError,match='amended'):
        mga.validate_company(c,symbol='MGA',cik=mga.CIK,observed_at=NOW)


def test_annual_amendment_before_latest_interim_is_held():
    c=company();r=c['filings']['recent']
    for key,value in zip(r,('0000749098-26-888888','40-F/A','2026-04-01','2025-12-31','amend.htm')):r[key].append(value)
    with pytest.raises(mga.MgaFinancialError,match='amended'):
        mga.validate_company(c,symbol='MGA',cik=mga.CIK,observed_at=NOW)


def test_comparative_change_reconciles_locally_but_cannot_mix_periods(monkeypatch):
    data=bundle(monkeypatch);tables=deepcopy(FIXTURE['current']['tables']);rows=tables[0]['rows']
    # Alter revenue and costs equally so the income statement still reconciles,
    # but its prior-year half disagrees with the separately retained prior report.
    for label,old,new in [('Sales','20,700','20,701'),('Cost of goods sold','17,954','17,955')]:
        row=next(r for r in rows if r[0]==label);row[row.index(old)]=new
    raw=render('current',tables);trust(monkeypatch,'current',raw)
    mga.parse(raw,source_key='current')
    data['mga_originals']['current']=raw
    with pytest.raises(mga.MgaFinancialError,match='restatement'):project(data)
