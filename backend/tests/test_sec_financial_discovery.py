"""Real filing discovery plus interruption, mutation and admission boundaries."""
from copy import deepcopy
from datetime import date,datetime,timedelta,timezone
from pathlib import Path
import gzip,hashlib,json,socket
import pytest
from app.services import sec_financial_discovery as discovery

ROOT=Path(__file__).parent/'fixtures/sec_financial_discovery'
MANIFEST=json.loads((ROOT/'sources.json').read_text())
OBSERVATION=json.loads((ROOT/'observation.json').read_text())
NOW=datetime.fromisoformat(OBSERVATION['observed_at'])
DAY=date.fromisoformat(OBSERVATION['available_by'])
COMPANY='https://data.sec.gov/submissions/CIK0001513845.json'
FACTS='https://data.sec.gov/api/xbrl/companyfacts/CIK0001513845.json'
STATEMENT='https://www.sec.gov/Archives/edgar/data/1513845/000110465926094844/nbis-20260812xex99d2.htm'
COVER=STATEMENT.replace('xex99d2.htm','x6k.htm')


def source(url):
    meta=MANIFEST[url];raw=gzip.decompress((ROOT/meta['file']).read_bytes())
    assert len(raw)==meta['bytes'] and hashlib.sha256(raw).hexdigest()==meta['sha256']
    return raw


class Client:
    def __init__(self,overrides=None):self.calls=[];self.overrides=overrides or {}
    def get(self,url):
        self.calls.append(url)
        value=self.overrides[url] if url in self.overrides else source(url)
        if isinstance(value,Exception):raise value
        return value


def advance(previous=None,client=None,company=None,**kwargs):
    return discovery.advance(company or source(COMPANY),symbol='NBIS',cik='0001513845',
        available_by=DAY,observed_at=NOW,client=client or Client(),previous=previous,**kwargs)


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    monkeypatch.setattr(socket.socket,'connect',lambda *a,**k:pytest.fail('Unexpected network'))


def complete():
    state=None;client=Client();runs=[]
    for _ in range(6):
        result=advance(state,client);runs.append(result['inspected_this_run']);state=result['state']
        if result['all_candidate_covers_observed']:break
    assert result['all_candidate_covers_observed']
    return result,client,runs


def test_complete_current_directory_discovers_hidden_original_with_bounded_progress():
    result,client,runs=complete()
    assert runs==[3,3,3,3,3] and result['candidate_count']==15
    assert len(client.calls)==len(set(client.calls))==15
    assert not result['unresolved_covers'] and not result['financial_publication_eligible']
    assert all(url.endswith('.htm') and 'ex99' not in url for url in client.calls)
    matching=[r for r in result['statement_candidates'] if STATEMENT in r['links']]
    assert len(matching)==1 and matching[0]['candidate']['cover_url']==COVER
    assert matching[0]['link_kinds']=={STATEMENT:'financial_statement'}
    assert matching[0]['cover_sha256']==MANIFEST[COVER]['sha256']
    assert client.calls.index(COVER)>=3  # Beyond the old fixed discovery window.
    repeat_client=Client();repeated=advance(result['state'],repeat_client)
    assert repeated['state']==result['state'] and repeated['inspected_this_run']==0
    assert not repeat_client.calls and not repeated['financial_publication_eligible']
    assert len(json.dumps(result['state']).encode())<16_000


def test_discovered_candidate_still_requires_full_original_normalization_and_repeat():
    from app.services import sec_tabular_reporting as reporting,sec_fundamentals as core,sec_financial_statements as panels
    result,_,_=complete();hint=next(r for r in result['statement_candidates'] if STATEMENT in r['links'])
    assert hint['candidate']['cik']=='0001513845'
    args=dict(source_url=STATEMENT,company_raw=source(COMPANY),cover_raw=source(COVER),
        cover_url=COVER,symbol='NBIS',cik='0001513845',available_by=DAY)
    normalized=reporting.normalize(source(STATEMENT),**args)
    assert normalized['evidence']['native_observation_count']==18
    assert normalized['evidence']['currency']=='USD' and normalized['evidence']['accounting_basis']=='US GAAP'
    supplement={'evidence':{'table_reporting':normalized['evidence']},'table_original':source(STATEMENT),'table_args':args}
    def project():
        common=dict(symbol='NBIS',cik='0001513845',observed_at=NOW,filing_supplement=supplement)
        return (core.project_fundamentals(source(FACTS),source(COMPANY),**common),
                panels.project(source(FACTS),source(COMPANY),**common))
    values,panel=project();assert (values,panel)==project()
    assert values['current_ratio']==pytest.approx(4.029839487029043)
    assert str(values['period_date'])=='2026-06-30'
    assert 'roe' not in values and 'net_debt_to_ebitda' not in values
    # Discovery receipts cannot substitute for original financial evidence.
    with pytest.raises(reporting.TabularReportingError):
        reporting.validate({'evidence':{'table_reporting':hint}},company_raw=source(COMPANY),symbol='NBIS',cik='0001513845',available_by=DAY)


def new_directory(*,amendment=False,changed_identity=False):
    company=json.loads(source(COMPANY));recent=company['filings']['recent']
    index=recent['accessionNumber'].index('0001104659-26-094844')
    row={key:values[index] for key,values in recent.items()}
    row.update(accessionNumber='0001104659-26-999999',form='6-K/A' if amendment else '6-K',
               filingDate=str(DAY),reportDate='2026-08-12',primaryDocument='new.htm')
    for key,values in recent.items():values.insert(0,row[key])
    if changed_identity:recent['primaryDocument'][index+1]='changed.htm'
    return json.dumps(company).encode()


@pytest.mark.parametrize('amendment',[False,True])
def test_new_filing_or_amendment_does_not_inherit_receipt_or_erase_unchanged_progress(amendment):
    original,_,_=complete();before=deepcopy(original['state']);company=new_directory(amendment=amendment)
    url='https://www.sec.gov/Archives/edgar/data/1513845/000110465926999999/new.htm'
    client=Client({url:b'<html><body><p>Routine update</p></body></html>'})
    result=advance(before,client,company)
    assert before==original['state'] and client.calls==[url]
    assert result['candidate_count']==16 and result['remaining']==0
    assert all(result['state']['receipts'][k]==r for k,r in before['receipts'].items())
    receipt=next(r for r in result['state']['receipts'].values() if r['candidate']['cover_url']==url)
    assert receipt['candidate']['form']==('6-K/A' if amendment else '6-K')


def test_unrelated_directory_update_preserves_progress_but_changed_candidate_does_not():
    original,_,_=complete();company=json.loads(source(COMPANY));company['phone']='Changed metadata'
    client=Client();repeat=advance(original['state'],client,json.dumps(company).encode())
    assert not client.calls and repeat['state']==original['state']
    recent=company['filings']['recent'];idx=recent['accessionNumber'].index('0001104659-26-094844')
    recent['primaryDocument'][idx]='changed.htm';url=COVER.rsplit('/',1)[0]+'/changed.htm'
    client=Client({url:b'<html><body>No identified statement</body></html>'})
    updated=advance(original['state'],client,json.dumps(company).encode())
    assert client.calls==[url] and len(updated['state']['receipts'])==15
    assert not any(r['candidate']['cover_url']==COVER for r in updated['state']['receipts'].values())


def test_transport_failure_does_not_commit_partial_batch_or_skip_denied_source():
    first=advance();previous=deepcopy(first['state'])
    rows=discovery._inventory(source(COMPANY),symbol='NBIS',cik='0001513845',available_by=DAY)
    client=Client({rows[4]['cover_url']:RuntimeError('HTTP 403')})
    with pytest.raises(RuntimeError,match='HTTP 403'):advance(previous,client)
    assert previous==first['state'] and len(client.calls)==2
    recovered=Client();second=advance(previous,recovered)
    assert recovered.calls==[r['cover_url'] for r in rows[3:6]]
    assert second['remaining']==9


@pytest.mark.parametrize('defect',['symbol','cik','sha','future','link','bytes','kind','candidate'])
def test_malformed_receipts_cannot_influence_next_scan(defect):
    first=advance()['state'];state=deepcopy(first);receipt=next(iter(state['receipts'].values()))
    if defect in ('symbol','cik'):state[defect]='OTHER'
    elif defect=='sha':receipt['cover_sha256']='bad'
    elif defect=='future':receipt['observed_at']=(NOW+timedelta(days=1)).isoformat()
    elif defect=='link':receipt['links']=['https://example.com/financials.htm'];receipt['link_kinds']={receipt['links'][0]:'financial_statement'}
    elif defect=='bytes':receipt['bytes']=-1
    elif defect=='kind':receipt['link_kinds']={u:'verified_financials' for u in receipt['links']}
    else:receipt['candidate']['form']='10-K'
    client=Client()
    with pytest.raises(discovery.DiscoveryError):advance(state,client)
    assert not client.calls and first!=state


@pytest.mark.parametrize('body,status',[
    (b'<html><head><base href="https://example.com/"></head><body></body></html>','cover_scope_hold'),
    (b'<html><body><a href="https://example.com/financials.htm">Financial statements</a></body></html>','link_scope_hold'),
    (b'<html><body><a href="other.htm?replacement=1">Financial statements</a></body></html>','link_scope_hold'),
    (b'x'*(discovery.MAX_COVER_BYTES+1),'cover_size_hold'),
],ids=['base_url','external_link','query_link','oversized_cover'])
def test_unresolved_covers_stay_explicit_and_never_qualify_values(body,status):
    rows=discovery._inventory(source(COMPANY),symbol='NBIS',cik='0001513845',available_by=DAY)
    client=Client({r['cover_url']:body for r in rows[:3]});result=advance(client=client)
    assert len(result['unresolved_covers'])==3
    assert {r['status'] for r in result['unresolved_covers']}=={status}
    assert not result['statement_candidates'] and not result['financial_publication_eligible']


def test_wrong_sgml_wrapper_identity_is_held_without_admitting_its_link():
    rows=discovery._inventory(source(COMPANY),symbol='NBIS',cik='0001513845',available_by=DAY)
    raw=source(COVER)
    client=Client({r['cover_url']:raw for r in rows[:3]})
    result=advance(client=client)
    assert {r['status'] for r in result['unresolved_covers']}=={'cover_scope_hold'}
    assert not result['statement_candidates']


def test_ambiguous_links_and_announcement_dates_are_not_financial_admission():
    rows=discovery._inventory(source(COMPANY),symbol='NBIS',cik='0001513845',available_by=DAY)
    raw=b'<html><body><a href="date.htm">Date of earnings release</a><a href="one.htm">Financial statements</a><a href="two.htm">Financial statements</a></body></html>'
    result=advance(client=Client({r['cover_url']:raw for r in rows[:3]}))
    assert all(len(r['links'])==2 and all('date.htm' not in u for u in r['links']) for r in result['statement_candidates'])
    assert not result['financial_publication_eligible']


def test_parser_version_change_restarts_observation_without_claiming_complete():
    state=advance()['state'];state['version']='prior_parser_version';client=Client()
    result=advance(state,client)
    assert len(client.calls)==3 and result['remaining']==12


def test_candidate_and_state_bounds_fail_before_source_calls(monkeypatch):
    client=Client();monkeypatch.setattr(discovery,'MAX_CANDIDATES',2)
    with pytest.raises(discovery.DiscoveryError,match='candidate_bound'):advance(client=client)
    assert not client.calls
