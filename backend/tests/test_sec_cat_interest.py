from copy import deepcopy
from datetime import date, datetime, timezone
import gzip
import hashlib
import json
from pathlib import Path
import pytest
from app.clients.direct_sources import DirectSourceError, DirectSourceClient
from app.services import sec_cat_interest as cat, sec_fundamentals as sec

FOLDER = Path(__file__).parent/'fixtures/sec_cat_interest'
NOW = datetime(2026,10,9,3,26,7,tzinfo=timezone.utc)

@pytest.fixture(scope='module')
def originals():
    manifest = json.loads((FOLDER/'manifest.json').read_bytes())
    result = {}
    for row in manifest['sources']:
        raw = gzip.decompress((FOLDER/row['file']).read_bytes())
        assert hashlib.sha256(raw).hexdigest() == row['sha256']
        result[row['name']] = raw
    return result, manifest

class Source:
    def __init__(self, originals, fail_at=None):
        self.files, self.manifest = originals
        self.calls, self.fail_at = [], fail_at
    def get(self,url):
        self.calls.append(url)
        if self.fail_at == len(self.calls):
            raise DirectSourceError('Source unavailable')
        names = {row['url']:row['name'] for row in self.manifest['sources']}
        assert url in names, 'Unexpected source request: '+url
        return self.files[names[url]]

def project(originals, **kwargs):
    files,_ = originals
    return sec.project_fundamentals(files['CAT-facts.json'],files['CAT-company.json'],
        symbol='CAT',cik=cat.CIK,observed_at=NOW,**kwargs)

@pytest.fixture(scope='module')
def proof(originals):
    client = Source(originals)
    before = project(originals)
    acquired = cat.acquire(before,originals[0]['CAT-company.json'],symbol='CAT',cik=cat.CIK,observed_at=NOW,client=client)
    assert acquired['evidence']['status'] == 'originals_verified'
    return before, acquired, client.calls

def test_complete_originals_preserve_canonical_and_nonleverage_values(originals,proof):
    before,acquired,calls = proof
    assert len(calls) == len(set(calls)) == 2
    after = project(originals,interest_supplement=acquired)
    assert after == project(originals,interest_supplement=acquired)
    assert {k:v for k,v in before.items() if k not in {'source_evidence_json','net_debt_to_ebitda'}} == {k:v for k,v in after.items() if k not in {'source_evidence_json','net_debt_to_ebitda'}}
    a,b = [json.loads(v['source_evidence_json']) for v in (before,after)]
    old,new = a.pop('leverage_preparation'),b.pop('leverage_preparation')
    old_policy,new_policy = a.pop('net_debt_policy'),b.pop('net_debt_policy')
    assert old_policy.pop('reason') == 'incomplete_or_nonpositive_ebitda'
    assert old_policy['status'] == 'unavailable'
    assert new_policy['status'] == 'available'
    assert new_policy['candidate']['debt'] == 45_146_000_000
    assert new_policy['candidate']['cash'] == 6_713_000_000
    assert new_policy['value'] == (45_146_000_000-6_713_000_000)/18_426_000_000
    assert b['calculations'].pop('net_debt_to_ebitda') == new_policy['formula']
    assert a == b
    assert old['ebitda']['missing_components'] == ['leverage_interest']
    assert new['ebitda']['status'] == 'components_complete'
    assert new['ebitda']['value'] == 18_426_000_000
    assert new['ebitda']['components']['leverage_interest']['value'] == 1_933_000_000
    assert {(p['start'],p['end']) for p in new['interest_reconciliation']['accepted_periods']} == {
        ('2023-01-01','2023-12-31'),('2024-01-01','2024-12-31'),('2025-01-01','2025-12-31'),
        ('2025-04-01','2025-06-30'),('2026-04-01','2026-06-30'),
        ('2025-01-01','2025-06-30'),('2026-01-01','2026-06-30')}
    assert after['net_debt_to_ebitda'] == new_policy['value']
    for point in new['ebitda']['components'].values():
        assert all(p['source_cik'] == cat.CIK and p['source_sha256'] for p in point['inputs'])

@pytest.mark.parametrize('before,after',[
    (b'cat:FinancialProductsMember',b'cat:OtherMember'),
    (b'</xbrldi:explicitMember></xbrli:segment>',b'</xbrldi:explicitMember><other:explicitMember dimension="other:Axis">other:Member</other:explicitMember></xbrli:segment>'),
    (b'srt:ProductOrServiceAxis',b'srt:ConsolidationItemsAxis'),
    (b'Interest expense excluding Financial Products',b'Interest expense including Financial Products'),
    (b'http://www.cat.com/20260630',b'http://other.example/20260630'),
    (b'0000018230',b'0000018231'),
    (b'iso4217:USD',b'iso4217:EUR'),
    (b'name="us-gaap:FinancingInterestExpense"',b'name="us-gaap:FinancingInterestExpense" sign="-"'),
    (b'name="us-gaap:InterestExpenseNonoperating"',b'name="us-gaap:OtherExpense"'),
])
def test_original_scope_and_missing_component_rejected(originals,before,after):
    files,manifest = originals
    row = next(row for row in manifest['sources'] if row['name']=='interim.html')
    raw = files['interim.html']; assert before in raw
    raw = raw.replace(before,after)
    with pytest.raises((cat.CatInterestError,cat.inline.InlineFactsError)):
        cat.parse(raw,{**row,'sha256':hashlib.sha256(raw).hexdigest()})

def test_generic_segment_exclusion_is_unchanged(originals):
    files,manifest = originals
    row = next(row for row in manifest['sources'] if row['name']=='interim.html')
    _,standard = cat.parse(files['interim.html'],row)
    assert 'InterestExpenseNonoperating' not in standard['facts']
    assert 'FinancingInterestExpense' not in standard['facts']
    with pytest.raises(cat.CatInterestError,match='source_hash'):
        cat.parse(files['interim.html']+b' ',row)

@pytest.mark.parametrize('change',['symbol','cik','completed','other_missing','stale','future','shape','ambiguous','amended','unsafe_filename'])
def test_request_guards_before_transport(originals,proof,change):
    values = deepcopy(proof[0]); evidence=json.loads(values['source_evidence_json'])
    company = json.loads(originals[0]['CAT-company.json'])
    args = dict(symbol='CAT',cik=cat.CIK,observed_at=NOW,client=Source(originals))
    recent = company['filings']['recent']; target=recent['accessionNumber'].index('0000018230-26-000046')
    if change=='symbol':args['symbol']='OTHER'
    if change=='cik':args['cik']='2'
    if change=='completed':evidence['leverage_preparation']['ebitda']['status']='components_complete'
    if change=='other_missing':evidence['leverage_preparation']['ebitda']['missing_components'].append('leverage_da')
    if change=='stale':evidence['period_end']='2025-01-01'
    if change=='future':evidence['available_by']='2099-01-01'
    if change=='shape':recent['form'].pop()
    if change in {'ambiguous','amended'}:
        for k in ('accessionNumber','form','filingDate','reportDate','primaryDocument'):
            recent[k].append(recent[k][target])
        recent['primaryDocument'][-1]='different.htm'
        if change=='amended':
            recent['form'][-1]='10-Q/A';recent['filingDate'][-1]='2026-08-06';recent['accessionNumber'][-1]='0000018230-26-000099'
    if change=='unsafe_filename':recent['primaryDocument'][target]='../other.htm'
    values['source_evidence_json']=json.dumps(evidence)
    acquired=cat.acquire(values,json.dumps(company).encode(),**args)
    assert not args['client'].calls
    assert acquired is None or acquired['evidence']['status']=='unavailable'

@pytest.mark.parametrize('fail_at',[1,2])
def test_optional_source_failure_preserves_all_values(originals,proof,fail_at):
    client=Source(originals,fail_at=fail_at)
    result=cat.supplement(proof[0],originals[0]['CAT-facts.json'],originals[0]['CAT-company.json'],
        symbol='CAT',cik=cat.CIK,observed_at=NOW,client=client)
    a,b=[json.loads(v['source_evidence_json']) for v in (proof[0],result)]
    evidence=b['leverage_preparation'].pop('interest_reconciliation')
    assert evidence['reason']=='source_unavailable' and evidence['status']=='unavailable'
    assert a==b and len(client.calls)==fail_at

@pytest.mark.parametrize('change',['cik','future','companion','hash','reported_conflict'])
def test_apply_preserves_conflicts_and_source_binding(originals,proof,change):
    acquired=deepcopy(proof[1]); key=(date(2026,1,1),date(2026,6,30))
    if change=='cik':acquired['evidence']['cik']='0000000002'
    if change=='future':acquired['evidence']['documents'][0]['filed']='2099-01-01'
    if change=='companion':acquired['points']={k:{**v,'source_cik':'0000000002'} for k,v in acquired['points'].items()}
    if change=='hash':acquired['records'][key]['source_sha256']='0'*64
    if change=='reported_conflict':
        records={'leverage_interest':{key:None}}
        result,_=cat.apply(records,acquired,symbol='CAT',cik=cat.CIK,available_by=date(2026,10,7))
        assert result==records
    else:
        result=project(originals,interest_supplement=acquired)
        e=json.loads(result['source_evidence_json'])['leverage_preparation']['ebitda']
        assert e['status']=='incomplete'

def test_preparation_cache_and_raw_fetch_entry_points(originals,proof,monkeypatch):
    from sqlalchemy import create_engine,select,func
    from sqlalchemy.orm import sessionmaker
    from app.db import Base
    from app.models import InsightsSnapshot,FundamentalsCache,Event
    from app.services import sec_fundamentals_preparation as prep, sec_directory
    class Clock(datetime):
        @classmethod
        def now(cls,tz=None):return NOW
    monkeypatch.setattr(prep,'datetime',Clock);monkeypatch.setattr(sec,'datetime',Clock)
    monkeypatch.setenv('SEC_FUNDAMENTALS_WARMING_ENABLED','1')
    monkeypatch.setenv('FUNDAMENTALS_PROVIDER','fmp')
    monkeypatch.setenv('STOCK_PRICE_PROVIDER','fmp')
    identity={'CAT':{'cik':cat.CIK,'name':'CATERPILLAR INC','symbol':'CAT','exchange':'NYSE'}}
    monkeypatch.setattr(sec_directory,'directory',lambda:identity)
    monkeypatch.setattr(sec,'company_directory',lambda day:identity)
    client=Source(originals)
    monkeypatch.setattr(DirectSourceClient,'get',lambda self,url:client.get(url))
    engine=create_engine('sqlite:///:memory:');Base.metadata.create_all(engine);factory=sessionmaker(bind=engine)
    monkeypatch.setattr(prep,'SessionLocal',factory)
    try:
        result=prep.prepare('CAT')
        assert result['calculation_evidence']['leverage_preparation']['ebitda']['value']==18_426_000_000
        assert len(client.calls)==4
        assert prep.prepare('CAT')==result and len(client.calls)==4
        with factory() as db:
            assert db.scalar(select(func.count()).select_from(InsightsSnapshot))==1
            assert db.scalar(select(func.count()).select_from(FundamentalsCache))==0
            assert db.scalar(select(func.count()).select_from(Event))==0
        direct=sec.fetch_fundamentals('CAT')
        assert json.loads(direct['source_evidence_json'])['leverage_preparation']['ebitda']['value']==18_426_000_000
        assert len(client.calls)==8
        assert prep.version_for_symbol('CAT')==prep.VERSION+'_'+cat.VERSION
        assert prep.version_for_symbol('ABC')==prep.VERSION
    finally:engine.dispose()
