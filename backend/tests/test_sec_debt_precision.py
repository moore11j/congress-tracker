from copy import deepcopy
from datetime import date
import json
import pytest
from app.services import sec_debt_precision as precision, sec_inline_financials as inline, sec_fundamentals as sec, sec_leverage
from app.clients.direct_sources import DirectSourceError
from test_sec_inline_financials import document, fact, ACC, URL, NOW
from test_sec_leverage import prepared_sources

TAGS = ('DebtAndCapitalLeaseObligations','DebtCurrent','LongTermDebtAndCapitalLeaseObligations')
END = date(2026,6,30)


def fixture_sources(total=90400, current=6117, noncurrent=84264):
    data,company = prepared_sources()
    company['filings']={'recent':dict(accessionNumber=[ACC],form=['10-Q'],filingDate=['2026-08-01'],reportDate=['2026-06-30'],primaryDocument=['report.htm'])}
    for tag,value in zip(TAGS,(total,current,noncurrent)):
        data['facts']['us-gaap'][tag]={'units':{'USD':[dict(val=value*1e6,end='2026-06-30',filed='2026-08-01',accn=ACC,form='10-Q')]}}
    return data,company


def original(total='90.4',decimals='-8'):
    extra='<xbrli:context id="balance"><xbrli:entity><xbrli:identifier scheme="http://www.sec.gov/CIK">1</xbrli:identifier></xbrli:entity><xbrli:period><xbrli:instant>2026-06-30</xbrli:instant></xbrli:period></xbrli:context>'
    extra+=fact(total,ctx='balance',tag=TAGS[0],attrs=f'scale="9" decimals="{decimals}"')
    for tag,value in zip(TAGS[1:],('6117','84264')):
        extra+=fact(value,ctx='balance',tag=tag,attrs='scale="6" decimals="-6"')
    return document(extra)


class Source:
    def __init__(self,raw=None,error=None):self.raw=raw or original();self.error=error;self.reads=[]
    def get(self,url):
        self.reads.append(url)
        assert url==URL
        if self.error:raise self.error
        return self.raw


def acquire(data,company,source):
    return inline.acquire(json.dumps(data).encode(),json.dumps(company).encode(),symbol='ABC',cik='1',observed_at=NOW,client=source,include_debt_precision=True)


def project(data,company,supplement=None):
    return sec.project_fundamentals(json.dumps(data).encode(),json.dumps(company).encode(),symbol='ABC',cik='1',observed_at=NOW,filing_supplement=supplement)


def instant_records(data):
    return {name:sec._records(data,'equity',date(2026,10,10),concepts=(tag,)) for name,tag in zip(precision.NAMES,TAGS)}


def annotated():
    data,company=fixture_sources();supplement=acquire(data,company,Source());records=instant_records(data)
    inline.annotate(records,supplement,expected_cik='1',available_by=date(2026,10,10))
    return {k:next(iter(v.values())) for k,v in records.items()}


def test_complete_conflicting_totals_fetch_one_original_without_replacing_core():
    data,company=fixture_sources();before=deepcopy(data);client=Source();supplement=acquire(data,company,client)
    assert client.reads==[URL] and supplement['debt_precision_only']
    assert data==before and inline.merge(data,supplement,available_by=date(2026,10,10)) is data
    baseline=project(data,company);candidate=project(data,company,supplement)
    assert {k:v for k,v in baseline.items() if k!='source_evidence_json'}=={k:v for k,v in candidate.items() if k!='source_evidence_json'}
    a=json.loads(baseline['source_evidence_json']);b=json.loads(candidate['source_evidence_json'])
    pa=a.pop('leverage_preparation');pb=b.pop('leverage_preparation');b.pop('inline_filing_supplement')
    assert a==b and pa['ebitda']==pb['ebitda']
    proof=pb['debt_precision_reconciliation']
    assert proof['status']=='contained_under_round_to_nearest'
    assert proof['reported_total_interval']==['90350000000.0','90450000000.0']
    assert proof['maturity_sum_interval']==['90380000000.0','90382000000.0']
    assert proof['canonical_selection'] is False
    assert candidate==project(data,company,supplement)
    assert 'net_debt_to_ebitda' not in candidate


@pytest.mark.parametrize('change',['equal','missing','filed','accession','currency','future','directory','ambiguous'])
def test_no_fetch_for_equal_missing_or_unbound_totals(change):
    data,company=fixture_sources();row=data['facts']['us-gaap'][TAGS[1]]['units']['USD'][0]
    if change=='equal':data['facts']['us-gaap'][TAGS[0]]['units']['USD'][0]['val']=90381e6
    if change=='missing':del data['facts']['us-gaap'][TAGS[1]]
    if change=='filed':row['filed']='2026-08-02'
    if change=='accession':row['accn']='other'
    if change=='currency':data['facts']['us-gaap'][TAGS[1]]['units']['EUR']=data['facts']['us-gaap'][TAGS[1]]['units'].pop('USD')
    if change=='future':row['filed']='2026-12-01'
    if change=='directory':company['filings']['recent']['primaryDocument']=[]
    if change=='ambiguous':
        for k,values in company['filings']['recent'].items():values.append('other.htm' if k=='primaryDocument' else values[0])
    client=Source()
    if change=='directory':
        with pytest.raises(inline.InlineFactsError,match='filing_directory_shape'):acquire(data,company,client)
    else:
        assert acquire(data,company,client) is None
    assert client.reads==[]


@pytest.mark.parametrize('filename',['../report.htm','https://elsewhere.test/a.htm','report.xml'])
def test_invalid_directory_path_never_fetches(filename):
    data,company=fixture_sources();company['filings']['recent']['primaryDocument']=[filename]
    client=Source();result=acquire(data,company,client)
    assert not client.reads and result['evidence']['reason']=='filing_directory_identity'


@pytest.mark.parametrize('error',[DirectSourceError('unavailable'),inline.InlineFactsError('bad source')])
def test_optional_failure_keeps_complete_existing_core(error):
    data,company=fixture_sources();client=Source(error=error);supplement=acquire(data,company,client)
    assert len(client.reads)==1 and supplement['evidence']['status']=='unavailable'
    a=project(data,company);b=project(data,company,supplement)
    assert {k:v for k,v in a.items() if k!='source_evidence_json'}=={k:v for k,v in b.items() if k!='source_evidence_json'}
    proof=json.loads(b['source_evidence_json'])['leverage_preparation']['debt_precision_reconciliation']
    assert proof['status']=='original_precision_unavailable'


@pytest.mark.parametrize('change',['value','issuer','filed','future','accession','sha','decimals'])
def test_annotations_require_exact_original_value_identity_and_precision(change):
    data,company=fixture_sources();supplement=acquire(data,company,Source());records=instant_records(data)
    if change=='value':next(iter(records[precision.NAMES[0]].values()))['value']+=1
    if change=='issuer':supplement['evidence']['cik']='0000000002'
    if change in {'filed','future'}:supplement['evidence']['filed']='2026-12-01' if change=='future' else '2026-08-02'
    if change=='accession':supplement['evidence']['accession']='other'
    if change=='sha':supplement['evidence']['sha256']='0'*64
    if change=='decimals':
        for p in supplement['provenance'].values():
            if 'reported_precision' in p:p['reported_precision']['decimals']='NaN'
    inline.annotate(records,supplement,expected_cik='1',available_by=date(2026,10,10))
    result=precision.reconcile({k:next(iter(v.values())) for k,v in records.items()})
    assert result['status']=='original_precision_unavailable'


@pytest.mark.parametrize('raw',[None,'','NaN','Infinity','-21','21','1.0',1,True])
def test_unsupported_precision_is_not_inferred(raw):
    points=annotated();points[precision.NAMES[0]]['reported_precision']['decimals']=raw
    assert precision.reconcile(points)['status']=='original_precision_unavailable'


@pytest.mark.parametrize('change',['different_original','mismatched_tag','mismatched_unit','mismatched_cik','derived'])
def test_bound_precision_cannot_be_reused_for_another_fact(change):
    points=annotated();point=points[precision.NAMES[0]];p=point['reported_precision']
    if change=='different_original':p['source_sha256']='f'*64
    if change=='mismatched_tag':p['tag']='Other'
    if change=='mismatched_unit':p['unit']='CAD'
    if change=='mismatched_cik':point['source_cik']='0000000002'
    if change=='derived':p['derived']=True
    assert precision.reconcile(points)['status'] in {'original_precision_unavailable','incompatible_originals','incomplete_or_incompatible'}


def test_merely_overlapping_intervals_are_not_containment_or_accounting_proof():
    points=annotated();p=points[precision.NAMES[0]];p['value']=90381e6;p['reported_precision'].update(value=p['value'],decimals='-6')
    assert precision.reconcile(points)['status']=='not_contained'
    p['reported_precision']['decimals']='INF'
    assert precision.reconcile(points)['status']=='not_contained'


def test_precision_does_not_relax_double_counted_lease_addition():
    points=annotated();template=points[precision.NAMES[0]]
    points['leverage_borrowings']={**template,'value':90381e6,'tag':'DebtLongtermAndShorttermCombinedAmount'}
    points['leverage_finance_leases']={**template,'value':19e6,'tag':'FinanceLeaseLiability'}
    assert sec_leverage._debt_addition_checks(points)['leverage_borrowings']['status']=='conflicting_debt_addition'
    assert precision.reconcile(points)['status']=='contained_under_round_to_nearest'


from test_sec_fundamentals_preparation import fixture


def test_actual_comcast_original_cache_repeat_and_no_canonical_writes(fixture,monkeypatch):
    import gzip,hashlib
    from pathlib import Path
    from sqlalchemy import select,func
    from app.models import FundamentalsCache,Event,InsightsSnapshot
    from app.clients.direct_sources import DirectSourceClient
    from app.services import sec_fundamentals_preparation as prep,sec_directory
    folder=Path(__file__).parent/'fixtures/sec_debt_precision'
    manifest=json.loads((folder/'manifest.json').read_bytes());responses={}
    for row in manifest['sources']:
        raw=gzip.decompress((folder/(row['file']+'.gz')).read_bytes())
        assert hashlib.sha256(raw).hexdigest()==row['sha256']
        responses[row['url']]=raw
    factory,_,_,calls=fixture
    directory={'fields':['cik','name','ticker','exchange'],'data':[[1166691,'COMCAST CORP','CMCSA','Nasdaq']]}
    def get(client,url):
        calls.append(url)
        return json.dumps(directory).encode() if url==sec_directory.URL else responses[url]
    monkeypatch.setattr(DirectSourceClient,'get',get)
    result=prep.prepare('CMCSA')
    assert len(calls)==4 and result['status']=='partial'
    panel=result['calculation_evidence']['leverage_preparation']
    proof=panel['debt_precision_reconciliation']
    assert proof['status']=='contained_under_round_to_nearest'
    assert [point['value'] for point in proof['inputs']]==[90400e6,6117e6,84264e6]
    assert [point['reported_precision']['decimals'] for point in proof['inputs']]==['-8','-6','-6']
    assert panel['ebitda']['value']==34131e6
    assert result['values']['net_debt_to_ebitda'] == pytest.approx((90381e6 - 7661e6) / 34131e6)
    assert result['calculation_evidence']['net_debt_policy']['rounding_reconciliation']['canonical_selection'] is True
    with factory() as db:
        payload=db.get(InsightsSnapshot,'sec-current-fundamentals:CMCSA:v1').payload_json
        assert db.scalar(select(func.count()).select_from(FundamentalsCache))==0
        assert db.scalar(select(func.count()).select_from(Event))==0
    monkeypatch.setattr(DirectSourceClient,'get',lambda *args:pytest.fail('Cached repeat attempted source request'))
    assert prep.prepare('CMCSA')==result
    with factory() as db:assert db.get(InsightsSnapshot,'sec-current-fundamentals:CMCSA:v1').payload_json==payload


def test_statement_acquisition_default_does_not_fetch_precision_original():
    data,company=fixture_sources();client=Source()
    assert inline.acquire(json.dumps(data).encode(),json.dumps(company).encode(),symbol='ABC',cik='1',observed_at=NOW,client=client) is None
    assert client.reads==[]


@pytest.mark.parametrize('url',['https://example.com/report.htm','https://www.sec.gov/Archives/edgar/data/2/000000000126000002/report.htm',None])
def test_precision_source_url_must_bind_the_same_issuer_and_accession(url):
    points=annotated();points[precision.NAMES[0]]['reported_precision']['source_url']=url
    assert precision.reconcile(points)['status']=='original_precision_unavailable'


def test_excess_reported_digits_are_not_rounding_evidence():
    points=annotated();p=points[precision.NAMES[0]];p['value']+=1;p['reported_precision']['value']=p['value']
    assert precision.reconcile(points)['status']=='digits_exceed_reported_precision'


@pytest.mark.parametrize('available',[date(2027,1,1),date(2026,6,29)])
def test_stale_or_future_balance_never_adds_a_precision_request(available):
    data,company=fixture_sources()
    assert precision.request(data,company=company,available_by=available,end=END,forms={'10-Q'}) is None
