"""Rollover collection cannot bypass identity, reviewed policies or publication."""
import json
from copy import deepcopy
import pytest
from app.models import InsightsSnapshot
from app.services import sec_fundamentals_preparation as prep
from app.services import sec_financial_discovery_reader as reader
from test_sec_financial_discovery_store import database
from test_sec_financial_discovery import NOW


def save(database,payload,source='sec_edgar'):
    with database() as db:
        db.merge(InsightsSnapshot(kind='sec-current-fundamentals:SAP:v1',source=source,
            fetched_at=NOW,payload_json=json.dumps(payload)))
        db.commit()


def payload():
    return dict(symbol='SAP',source='sec_edgar',version=prep.version_for_symbol('SAP'),
        source_evidence={'cik':'0001000184'},status='unavailable',values={},
        reason='unsupported_current_financial_coverage',detail='sap_newer_or_amended_report_requires_review')


@pytest.mark.parametrize('symbol,cik,detail',[
    ('SAP','0001000184','sap_newer_or_amended_report_requires_review'),
    ('NVO','0000353278','nvo_new_or_amended_report_requires_review')])
def test_current_rollover_can_collect_but_cannot_enable_publication(database,monkeypatch,symbol,cik,detail):
    monkeypatch.setattr(prep,'SessionLocal',database)
    monkeypatch.setenv('SEC_FINANCIAL_DISCOVERY_READ_ENABLED','0')
    p=payload();p.update(symbol=symbol,version=prep.version_for_symbol(symbol),detail=detail,source_evidence={'cik':cik})
    with database() as db:
        db.add(InsightsSnapshot(kind=f'sec-current-fundamentals:{symbol}:v1',source='sec_edgar',
            fetched_at=NOW,payload_json=json.dumps(p)));db.commit()
    assert prep._continue_discovery(symbol,cik)
    assert reader.prepared_choice(b'',b'',symbol=symbol,cik=cik,available_by=NOW.date(),observed_at=NOW,client=None) is None


@pytest.mark.parametrize('mutation',[
    {'source':'fmp'},{'symbol':'NVO'},{'version':'obsolete'},
    {'source_evidence':{'cik':'0000353278'}},{'source_evidence':None},
    {'status':'partial'},{'values':{'roe':12}}, {'reason':'invalid_company_identity'},
    {'detail':'sap_comparative_restatement_requires_review'},
    {'detail':'nvo_new_or_amended_report_requires_review'},{'detail':None},
    {'discovery':{},'discovery_version':'obsolete','detail':None},
])
def test_other_holds_and_wrong_identity_stay_excluded(database,monkeypatch,mutation):
    monkeypatch.setattr(prep,'SessionLocal',database)
    p=payload();p.update(deepcopy(mutation));save(database,p)
    assert not prep._continue_discovery('SAP','0001000184')


@pytest.mark.parametrize('status,values,reason',[
    ('unavailable',{},'financial_discovery_pending'),
    ('unavailable',{},'financial_discovery_held'),
    ('partial',{'current_ratio':1.2},None)])
def test_current_discovery_remains_reachable_after_initial_hold(database,monkeypatch,status,values,reason):
    monkeypatch.setattr(prep,'SessionLocal',database)
    p=payload();p.pop('detail');p.update(discovery={'phase':'covers'},discovery_version=reader.VERSION,
        status=status,values=values,reason=reason);save(database,p)
    assert prep._continue_discovery('SAP','0001000184')


@pytest.mark.parametrize('policy',['reviewed','basis','issuer','disabled','foreign_row'])
def test_existing_policy_exclusions_remain(database,monkeypatch,policy):
    from app.services import sec_reporting_policy,sec_currency_ratios
    monkeypatch.setattr(prep,'SessionLocal',database)
    save(database,payload(),source='fmp' if policy=='foreign_row' else 'sec_edgar')
    if policy=='reviewed':monkeypatch.setattr(sec_reporting_policy,'reviewed',lambda *a:True)
    if policy=='basis':monkeypatch.setattr(sec_currency_ratios,'basis',lambda *a:True)
    if policy=='issuer':monkeypatch.setitem(prep.ISSUER_PROFILES,'SAP',{})
    if policy=='disabled':monkeypatch.setenv('SEC_FINANCIAL_DISCOVERY_ENABLED','0')
    assert not prep._continue_discovery('SAP','0001000184')


def test_complete_sap_rollover_originals_collect_but_retain_semantic_holds(monkeypatch):
    from pathlib import Path
    from datetime import datetime
    import gzip,hashlib
    from app.services import sec_financial_discovery as discovery
    root=Path(__file__).parent/'fixtures/sec_rollover_discovery'
    meta=json.loads((root/'sources.json').read_text(encoding='utf-8'))
    originals={}
    for key,entry in meta['files'].items():
        raw=gzip.decompress((root/entry['file']).read_bytes())
        assert len(raw)==entry['bytes'] and hashlib.sha256(raw).hexdigest()==entry['sha256']
        originals[key]=raw
    company=originals.pop('company');facts=json.dumps({'cik':1000184}).encode()
    now=datetime.fromisoformat(meta['observed_at'])
    kw=dict(symbol='SAP',cik='0001000184',available_by=now.date(),observed_at=now)
    state=discovery.inspect(company,**kw)['state']
    candidates=discovery._inventory(company,symbol='SAP',cik=kw['cik'],available_by=now.date())
    assert len(candidates)==3
    for candidate in candidates:
        state['receipts'][discovery._fingerprint(candidate)]=discovery._observe(originals[candidate['cover_url']],candidate,now)
    class Client:
        def __init__(self):self.calls=[]
        def get(self,url):self.calls.append(url);return originals[url]
    client=Client()
    with pytest.raises(discovery.DiscoveryError,match='discovery_reviewed_policy_required'):
        reader.inspect_candidate(company,facts,checkpoint=state,accession=candidates[0]['accession'],client=client,**kw)
    assert not client.calls
    receipts=[]
    for candidate in candidates:
        args=dict(checkpoint=state,accession=candidate['accession'],annual_rollover=True,**kw)
        result=reader.inspect_candidate(company,facts,client=client,**args)
        assert result==reader.inspect_candidate(company,facts,client=Client(),**args)
        receipts.append(result['receipt'])
        state['receipts'][discovery._fingerprint(candidate)]['qualification']=result['receipt']
    assert [r['status'] for r in receipts]==['held','held','no_supported_statement']
    assert receipts[0]['holds'][0]['reason']=='table_reporting_currency_unproved'
    assert receipts[1]['holds'][0]['reason']=='table_exhibit_ambiguous'
    assert all(r['financial_publication_eligible'] is False for r in receipts)
    assert reader.selection_plan(company,checkpoint=state,**kw)['reason']=='no_qualified_statement'
    assert len(set(client.calls))==4


@pytest.mark.parametrize('label,expected',[
    ('Quarterly Statement dated October 21, 2026',True),
    ('Half-Year Report dated July 23, 2026',True),
    ('Half Year Report',True),('Quarterly shareholder voting results',False),
    ('SAP AGM 2026 Voting Results dated May 5, 2026',False),
    ('Date for quarterly statement',False),('Will report quarterly statement',False)])
def test_periodic_statement_labels_are_hints_only(label,expected):
    from lxml import html
    from app.services.sec_tabular_reporting import statement_link_candidates
    tree=html.fromstring('<html><a href="ex99.htm">'+label+'</a></html>')
    result=statement_link_candidates(tree,'https://www.sec.gov/Archives/edgar/data/1/2/cover.htm')
    assert bool(result)==expected
    if result:assert result[0]['kind']=='financial_statement'


@pytest.mark.parametrize('currency,namespace',[('EUR','us-gaap'),('USD','ifrs-full')])
def test_original_statement_must_match_reviewed_annual_basis(monkeypatch,currency,namespace):
    from test_sec_tabular_reporting import originals
    from app.services import sec_foreign_statements,sec_tabular_reporting as tables
    raw,args,facts,urls=originals('NTB')
    monkeypatch.setattr(sec_foreign_statements,'profile',lambda *a:{'currency':currency,'namespace':namespace})
    with pytest.raises(tables.TabularReportingError,match='table_annual_reporting_basis_conflict'):
        tables.normalize(raw,**args)
