"""Complete SEC originals, isolated consumer replay and adversarial basis guards."""
from copy import deepcopy
from datetime import datetime,timedelta
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo
import gzip,hashlib,json,socket
import pytest
from lxml import html
from app.services import sec_comparative_basis as basis,sec_tabular_financials as extraction
from app.services import sec_tabular_reporting as tables,sec_financial_discovery as discovery
from app.services import sec_financial_discovery_reader as reader,sec_financial_discovery_store as store
from app.services import sec_fundamentals as core,sec_financial_statements as panels
from test_sec_financial_discovery_store import database

ROOT=Path(__file__).parent/'fixtures/sec_comparative_basis'
META=json.loads((ROOT/'sources.json').read_text(encoding='utf-8'))


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    monkeypatch.setattr(socket.socket,'connect',lambda *a,**k:pytest.fail('Offline source replay'))


def source(url):
    item=META['sources'][url];raw=gzip.decompress((ROOT/item['file']).read_bytes())
    assert len(raw)==item['bytes'] and hashlib.sha256(raw).hexdigest()==item['sha256']
    return raw


def originals(symbol='AUDC'):
    row=META['issuers'][symbol];candidate=row['candidate']
    now=datetime.fromisoformat(row['observed_at'])
    url=row['receipt']['originals'][-1]['url']
    args=dict(source_url=url,company_raw=source(row['company_url']),
        cover_raw=source(candidate['cover_url']),cover_url=candidate['cover_url'],symbol=symbol,cik=row['cik'],
        available_by=now.astimezone(ZoneInfo('America/New_York')).date()-timedelta(days=1))
    return source(url),args,now


def facts():return source('https://data.sec.gov/api/xbrl/companyfacts/CIK0001086434.json')


def inputs():
    raw,args,now=originals();tree=html.fromstring(raw)
    for node in tree.xpath('//script|//style|//template|//blockquote'):node.drop_tree()
    return extraction.visible_text(tree),extraction.extract(raw,**args),args,now


class Client:
    def __init__(self):self.calls=[]
    def get(self,url):self.calls.append(url);return source(url)


def supplement(company=None,now=None):
    raw,args,stamp=originals();stamp=now or stamp
    return tables.acquire(company or args['company_raw'],symbol='AUDC',cik=args['cik'],
        available_by=stamp.astimezone(ZoneInfo('America/New_York')).date()-timedelta(days=1),
        client=Client(),candidate_accessions=(META['issuers']['AUDC']['candidate']['accession'],),basis_facts_raw=facts())


def test_complete_original_values_provenance_repeat_and_cache():
    raw,args,now=originals();result=supplement()
    assert result==supplement()
    proof=result['evidence']['table_reporting'];comparative=proof['comparative_basis']
    assert proof['accounting_basis_origin']=='annual_comparative_reconciliation'
    assert proof['native_observation_count']==28
    assert comparative['annual']['accession']=='0001185185-26-001124'
    assert comparative['annual']['period']=='2025-12-31'
    assert [a['value'] for a in comparative['anchors']]==['181325000','323020000','81578000','171299000']
    kwargs=dict(symbol='AUDC',cik=args['cik'],observed_at=now,filing_supplement=result)
    values=core.project_fundamentals(facts(),args['company_raw'],**kwargs)
    assert values['current_ratio']==166892000/87975000
    assert not {'roe','net_margin','free_cash_flow','ev_to_ebitda'} & values.keys()
    panel=panels.project(facts(),args['company_raw'],**kwargs)
    assert panel==panels.project(facts(),args['company_raw'],**kwargs)
    assert panel['status']=='partial' and len(panel['quarterly'])==2
    assert [r['revenue'] for r in panel['quarterly']]==[61078000,62958000]
    assert [r['operatingCashFlow'] for r in panel['quarterly']]==[7725000,6096000]
    assert all(r['netIncome'] is None and r['eps'] is None and r['freeCashFlow'] is None for r in panel['quarterly'])
    assert not panel['annual'] and panel['summary']['revenueTtm'] is None
    assert 'annual comparative' in panel['reportingBasis']
    row=SimpleNamespace(source='sec_edgar',fetched_at=now,payload_json=json.dumps(panel))
    assert panels.cached_row_payload(row,'AUDC',now=now)==panel
    assert tables.cached_valid(proof,symbol='AUDC',cik=args['cik'])


def test_absent_annual_evidence_does_not_guess_basis_or_activate_reader(monkeypatch):
    raw,args,now=originals()
    with pytest.raises(tables.TabularReportingError,match='accounting_basis_unproved'):tables.normalize(raw,**args)
    monkeypatch.delenv('SEC_FINANCIAL_DISCOVERY_READ_ENABLED',raising=False)
    assert reader.prepared_choice(b'',b'',symbol='AUDC',cik=args['cik'],available_by=args['available_by'],observed_at=now,client=None) is None


@pytest.mark.parametrize('text',[
    'GAAP revenue.', 'Non-GAAP results.', 'The currency is US dollars.',
    'Results are not prepared in accordance with GAAP.',
    'Not results prepared in accordance with GAAP.',
    'Hypothetical results prepared in accordance with GAAP.',
    'Financial results will be prepared in accordance with GAAP.',
    'Results prepared in accordance with GAAP. Financial statements follow IFRS.',
    'Results prepared in accordance with GAAP. Taiwan GAAP applies.',
    'Results prepared in accordance with GAAP are hypothetical.',
    'Results prepared in accordance with GAAP applicable in Israel.',
])
def test_generic_negative_future_and_mixed_basis_cannot_qualify(text):
    assert basis.definition(text) is None


@pytest.mark.parametrize('defect',[
    'facts_identity','company_identity','currency','conflict','missing_printed','changed_printed',
    'missing_anchor','changed_anchor','duplicate_anchor','mixed_taxonomy','missing_income',
    'changed_income_period','amendment','later_annual','later_domestic','annual_date',
    'annual_path','annual_accession','directory_shape','future_statement','stale_statement',
])
def test_cross_filing_admission_requires_all_bindings(defect):
    text,observations,args,now=inputs();company=json.loads(args['company_raw']);f=json.loads(facts())
    recent=company['filings']['recent'];annual=recent['form'].index('20-F')
    accession=recent['accessionNumber'][annual];tag=f['facts']['us-gaap']['Assets']['units']['USD']
    point=next(p for p in tag if p.get('accn')==accession and p.get('end')=='2025-12-31')
    available=args['available_by']
    if defect=='facts_identity':f['cik']=1
    elif defect=='company_identity':company['cik']='0000000001'
    elif defect=='currency':observations['facts'][0]['currency']='CAD'
    elif defect=='conflict':observations['conflicts'].append({'metric':'revenue'})
    elif defect=='missing_printed':observations['facts']=[p for p in observations['facts'] if p['metric']!='total_assets']
    elif defect=='changed_printed':
        next(p for p in observations['facts'] if p['metric']=='total_assets' and p['end']=='2025-12-31')['value']+=1
    elif defect=='missing_anchor':f['facts']['us-gaap'].pop('Assets')
    elif defect=='changed_anchor':point['val']+=1
    elif defect=='duplicate_anchor':tag.append({**point,'val':point['val']+1})
    elif defect=='mixed_taxonomy':f['facts']['ifrs-full']={'Assets':{'units':{'USD':[deepcopy(point)]}}}
    elif defect=='missing_income':f['facts']['us-gaap'].pop('NetIncomeLoss')
    elif defect=='changed_income_period':
        for p in f['facts']['us-gaap']['NetIncomeLoss']['units']['USD']:
            if p.get('accn')==accession:p['start']='2025-10-01'
    elif defect=='amendment':recent['form'][annual]='20-F/A'
    elif defect=='later_annual':recent['reportDate'][annual]='2026-06-30';recent['filingDate'][annual]='2026-08-05'
    elif defect=='later_domestic':recent['form'][annual]='10-Q';recent['reportDate'][annual]='2026-06-30';recent['filingDate'][annual]='2026-08-01'
    elif defect=='annual_date':recent['filingDate'][annual]='2024-12-31'
    elif defect=='annual_path':recent['primaryDocument'][annual]='../other.htm'
    elif defect=='annual_accession':recent['accessionNumber'][annual]='not-an-accession'
    elif defect=='directory_shape':recent['form'].pop()
    elif defect=='future_statement':observations['source']['filed']='2027-01-01'
    elif defect=='stale_statement':available=available+timedelta(days=365)
    with pytest.raises(basis.ComparativeBasisError):
        basis.prove(text,observations,company_raw=json.dumps(company).encode(),facts_raw=json.dumps(f).encode(),available_by=available)


@pytest.mark.parametrize('defect',['facts','statement','basis','origin','binding','anchor','currency','date','missing_proof'])
def test_consumer_and_cache_reject_tampered_evidence(defect):
    raw,args,now=originals();result=supplement();current_facts=facts()
    proof=result['evidence']['table_reporting']
    if defect=='facts':current_facts+=b' '
    elif defect=='statement':result['table_original']=result['table_original'].replace(b'166,892',b'166,893')
    elif defect=='basis':proof['accounting_basis']='IFRS'
    elif defect=='origin':proof['accounting_basis_origin']='explicit'
    elif defect=='binding':proof['comparative_basis']['binding_sha256']='a'*64
    elif defect=='anchor':proof['comparative_basis']['anchors'][0]['value']='1'
    elif defect=='currency':proof['comparative_basis']['currency']='CAD'
    elif defect=='date':proof['comparative_basis']['annual']['period']='2027-12-31'
    elif defect=='missing_proof':proof.pop('comparative_basis')
    with pytest.raises(tables.TabularReportingError):
        tables.core(current_facts,args['company_raw'],symbol='AUDC',cik=args['cik'],observed_at=now,supplement=result)
    if defect not in {'facts','statement'}:assert not tables.cached_valid(proof,symbol='AUDC',cik=args['cik'])


@pytest.mark.parametrize('symbol',['CHKP','RSKD','ASX','TU','HMC'])
def test_other_complete_issuer_extractions_keep_identical_holds(symbol):
    raw,args,now=originals(symbol)
    for row in META['original_extractions']:
        if row['symbol']!=symbol:continue
        original=source(row['url']);original_args={**args,'source_url':row['url']}
        assert extraction.extract(original,**original_args)==row['baseline']
        # Even a full foreign issuer facts response cannot unlock wrong-identity tables.
        with pytest.raises(tables.TabularReportingError):
            tables.normalize(original,**original_args,basis_facts_raw=facts())


def test_parent_suffix_preserves_exact_entity_identity():
    assert extraction.company_key('AudioCodes Ltd. and its subsidiaries')==extraction.company_key('AudioCodes Ltd.')
    assert extraction.company_key('Other Co and its subsidiaries')!=extraction.company_key('AudioCodes Ltd.')
    assert extraction.company_key('AudioCodes Ltd. subsidiary Other Co')!=extraction.company_key('AudioCodes Ltd.')


def test_full_current_inventory_stays_pending_after_single_qualified_candidate():
    raw,args,now=originals();kw=dict(symbol='AUDC',cik=args['cik'],available_by=args['available_by'],observed_at=now)
    state=discovery.inspect(args['company_raw'],**kw)['state'];candidate=META['issuers']['AUDC']['candidate']
    cover=discovery._observe(args['cover_raw'],candidate,now);key=discovery._fingerprint(candidate);state['receipts'][key]=cover
    result=reader.inspect_candidate(args['company_raw'],facts(),checkpoint=state,accession=candidate['accession'],client=Client(),**kw)
    assert result['receipt']['status']=='qualified'
    cover['qualification']=result['receipt'];plan=reader.selection_plan(args['company_raw'],checkpoint=state,**kw)
    assert plan['status']=='pending' and plan['pending_covers']==5 and not plan['financial_publication_eligible']


def test_isolated_single_candidate_scenario_roundtrip_and_next_day(database,monkeypatch):
    # This intentionally restricted directory tests persistence, not real cohort completeness.
    raw,args,now=originals();company=json.loads(args['company_raw']);recent=company['filings']['recent']
    keep=[i for i,a in enumerate(recent['accessionNumber']) if a in {'0001185185-26-003250','0001185185-26-001124'}]
    company['filings']['recent']={k:[v[i] for i in keep] for k,v in recent.items()};company=json.dumps(company).encode()
    kw=dict(symbol='AUDC',cik=args['cik'],available_by=args['available_by'],observed_at=now)
    for _ in range(4):
        client=Client();result=store.progress(company,facts(),client=client,**kw)
        assert len(client.calls)<=6
        if result['phase']=='complete':break
    assert result['status']=='ready_for_original_revalidation'
    state=store.read('AUDC',args['cik']);restored=reader.restore(company,facts(),checkpoint=state,client=Client(),**kw)
    assert restored==supplement(company=company)
    assert reader.restore(company,facts(),checkpoint=state,client=Client(),**kw)==restored
    next_day={**kw,'available_by':args['available_by']+timedelta(days=1),'observed_at':now+timedelta(days=1)}
    store.progress(company,facts(),client=Client(),**next_day)
    tomorrow=reader.restore(company,facts(),checkpoint=store.read('AUDC',args['cik']),client=Client(),**next_day)
    today_values=tables.core(facts(),company,symbol='AUDC',cik=args['cik'],observed_at=now,supplement=restored)
    later_values=tables.core(facts(),company,symbol='AUDC',cik=args['cik'],observed_at=next_day['observed_at'],supplement=tomorrow)
    assert today_values['current_ratio']==later_values['current_ratio']
