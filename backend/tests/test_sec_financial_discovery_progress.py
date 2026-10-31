from copy import deepcopy
from datetime import datetime,timedelta,timezone
import gzip,hashlib,json
import pytest
from app.models import InsightsSnapshot
from app.services import sec_financial_discovery as discovery,sec_financial_discovery_store as store
from app.services import sec_financial_discovery_reader as reader,sec_fundamentals_preparation as prep
from test_sec_financial_discovery import ROOT,source,COMPANY,FACTS,NOW as COVER_NOW,DAY,COVER,STATEMENT,no_network
from test_sec_financial_discovery_store import database

EXHIBITS=json.loads((ROOT/'qualification-sources.json').read_text(encoding='utf-8'))
NOW=max(datetime.fromisoformat(r['observed_at']) for r in EXHIBITS.values())+timedelta(seconds=1)


class Client:
    def __init__(self):self.calls=[]
    def get(self,url):
        self.calls.append(url)
        if url not in EXHIBITS:return source(url)
        meta=EXHIBITS[url];raw=gzip.decompress((ROOT/meta['file']).read_bytes())
        assert hashlib.sha256(raw).hexdigest()==meta['sha256'] and len(raw)==meta['bytes']
        return raw


def progress(client=None,company=None,now=NOW):
    return store.progress(company or source(COMPANY),source(FACTS),symbol='NBIS',cik='0001513845',
        available_by=DAY,observed_at=now,client=client or Client())


def single_directory():
    company=json.loads(source(COMPANY));recent=company['filings']['recent']
    i=recent['accessionNumber'].index('0001104659-26-094844')
    company['filings']['recent']={k:[v[i]] for k,v in recent.items()}
    return json.dumps(company).encode()


def test_full_actual_cohort_advances_boundedly_and_corroborates_same_day_financials(database,monkeypatch):
    phases=[];qualifications=[]
    for _ in range(21):
        client=Client();result=progress(client)
        assert len(client.calls)<=6 and len(client.calls)==len(set(client.calls))
        phases.append(result['phase'])
        if result['phase']=='qualification':qualifications.extend(q['accession'] for q in result['qualifications'])
        if result['phase']=='complete':break
    assert phases[:5]==['covers']*5 and phases[-1]=='complete'
    assert 5<=phases.count('qualification')<=15 and len(phases)==6+phases.count('qualification')
    assert len(set(qualifications))==15
    assert result['status']=='ready_for_original_revalidation' and result['reason'] is None
    assert not result['financial_publication_eligible']
    assert result['selected_accessions']==['0001104659-26-094568','0001104659-26-094844']
    state=store.read('NBIS','0001513845');before=deepcopy(state)
    receipts=[r['qualification'] for r in state['receipts'].values()]
    assert sum(q['status']=='qualified' for q in receipts)==2
    assert sum(q['status']=='held' for q in receipts)==1
    client=Client();repeat=progress(client)
    assert repeat==result and not client.calls and store.read('NBIS','0001513845')==before
    restored=reader.restore(source(COMPANY),source(FACTS),symbol='NBIS',cik='0001513845',available_by=DAY,
        observed_at=NOW,checkpoint=state,client=client)
    assert len(client.calls)==4
    assert restored['evidence']['table_reporting']['discovery']['corroboration']['values_spliced'] is False
    monkeypatch.setenv('SEC_FINANCIAL_DISCOVERY_READ_ENABLED','1')
    from app.services import sec_inline_financials as inline,sec_fundamentals as core,sec_financial_statements as panels
    consumer_client=Client()
    integrated=inline.acquire(source(FACTS),source(COMPANY),symbol='NBIS',cik='0001513845',observed_at=NOW,client=consumer_client)
    assert integrated==restored and len(consumer_client.calls)==4
    args=dict(symbol='NBIS',cik='0001513845',observed_at=NOW)
    assert core.project_fundamentals(source(FACTS),source(COMPANY),filing_supplement=integrated,**args)['current_ratio']==pytest.approx(4.029839487029043)
    assert panels.project(source(FACTS),source(COMPANY),filing_supplement=integrated,**args)==panels.project(source(FACTS),source(COMPANY),filing_supplement=restored,**args)
    # An independently qualified conflicting same-day report must fail the
    # full-original agreement check, not win on URL or accession order.
    peer_url=next(u for u in EXHIBITS if '000110465926094568' in u)
    original=Client().get(peer_url)
    assert b'582.3' in original
    class Conflicting(Client):
        def get(self,url):
            raw=super().get(url)
            return raw.replace(b'582.3',b'583.3') if url==peer_url else raw
    peer=reader.inspect_candidate(source(COMPANY),source(FACTS),symbol='NBIS',cik='0001513845',available_by=DAY,
        observed_at=NOW,checkpoint=state,accession='0001104659-26-094568',client=Conflicting())['receipt']
    assert peer['status']=='qualified'
    state['receipts'][discovery._fingerprint(peer['candidate'])]['qualification']=peer
    from app.services.sec_tabular_reporting import TabularReportingError
    with pytest.raises(TabularReportingError,match='ambiguous'):
        reader.restore(source(COMPANY),source(FACTS),symbol='NBIS',cik='0001513845',available_by=DAY,
            observed_at=NOW,checkpoint=state,client=Conflicting())


def test_single_filing_scenario_restores_full_originals_and_rejects_drift(database):
    company=single_directory()
    assert progress(company=company)['phase']=='covers'
    qualified=progress(company=company)
    assert qualified['status']=='ready_for_original_revalidation' and qualified['phase']=='qualification'
    assert progress(company=company)['phase']=='complete'
    state=store.read('NBIS','0001513845');client=Client()
    kwargs=dict(symbol='NBIS',cik='0001513845',available_by=DAY,observed_at=NOW,checkpoint=state)
    result=reader.restore(company,source(FACTS),client=client,**kwargs)
    assert client.calls==[COVER,STATEMENT] and result['table_original']==source(STATEMENT)
    class Changed(Client):
        def get(self,url):return super().get(url)+(b' ' if url==STATEMENT else b'')
    with pytest.raises(discovery.DiscoveryError,match='original_changed'):
        reader.restore(company,source(FACTS),client=Changed(),**kwargs)
    # A new unseen filing must stop re-use even if the old financial bytes survive.
    with pytest.raises(discovery.DiscoveryError,match='incomplete'):
        reader.restore(source(COMPANY),source(FACTS),client=Client(),**kwargs)


@pytest.mark.parametrize('mutation',['hash','period','publication','source','candidate'])
def test_malformed_qualification_rejected_before_source_requests(database,mutation):
    company=single_directory();progress(company=company);progress(company=company)
    key=store.key_for('NBIS','0001513845')
    with database() as db:
        row=db.get(InsightsSnapshot,key);state=json.loads(row.payload_json)
        q=next(iter(state['receipts'].values()))['qualification']
        if mutation=='hash':q['originals'][0]['sha256']='broken'
        if mutation=='period':q['current_period']='2099-01-01'
        if mutation=='publication':q['financial_publication_eligible']=True
        if mutation=='source':q['originals'][0]['url']='https://example.com/source.htm'
        if mutation=='candidate':q['candidate']['cik']='0000000001'
        row.payload_json=json.dumps(state);db.commit()
    client=Client()
    with pytest.raises(discovery.DiscoveryError,match='qualification_invalid'):progress(client,company)
    assert not client.calls


def test_worker_preparation_uses_existing_slot_then_caches_pending_and_real_financials(database,monkeypatch):
    from app.services import sec_directory
    company=single_directory();clients=[];clock=[NOW]
    class Clock(datetime):
        @classmethod
        def now(cls,tz=None):return clock[0]
    class Transport(Client):
        def __init__(self):super().__init__();clients.append(self)
        def get(self,url):
            if url==COMPANY:self.calls.append(url);return company
            return super().get(url)
    monkeypatch.setattr(prep,'SessionLocal',database)
    monkeypatch.setattr(prep,'datetime',Clock)
    monkeypatch.setattr(prep,'DirectSourceClient',Transport)
    monkeypatch.setattr(sec_directory,'directory',lambda:{'NBIS':{'cik':'0001513845'}})
    monkeypatch.setenv('SEC_FUNDAMENTALS_WARMING_ENABLED','1')
    with database() as db:
        payload={'symbol':'NBIS','status':'unavailable','values':{},'reason':'unsupported_current_financial_coverage',
                 'observed_at':(NOW-timedelta(days=2)).isoformat()}
        db.add(InsightsSnapshot(kind='sec-current-fundamentals:NBIS:v1',source='sec_edgar',
            fetched_at=NOW-timedelta(days=2),payload_json=json.dumps(payload)));db.commit()
    for phase in ('covers','qualification','complete'):
        result=prep.prepare('NBIS',continue_discovery=True)
        assert result['discovery']['phase']==phase
        assert len(clients[-1].calls)<=8  # Existing two metadata + six original limit.
        before=len(clients)
        assert prep.prepare('NBIS',continue_discovery=True)==result and len(clients)==before
        clock[0]+=prep.DISCOVERY_RETRY_TTL+timedelta(seconds=1)
    assert result['status']=='partial' and result['values']['current_ratio']==pytest.approx(4.029839487029043)
    assert result['period_date']=='2026-06-30' and 'reason' not in result
    assert result['source']=='sec_edgar' and result['coverage']['complete'] is False


@pytest.mark.parametrize('suffix',['*','**','e','/2026',' (estimated)'])
def test_date_header_footnote_preserves_exact_year_without_accepting_other_suffixes(suffix):
    from lxml import html
    from app.services import sec_tabular_financials as tables
    from datetime import date
    url=next(u for u in EXHIBITS if '000110465926094568' in u)
    tree=html.fromstring(Client().get(url));table=tree.xpath('//table')[6]
    cells=[n for n in table.xpath('.//td|.//th') if tables.visible_text(n)=='2025*']
    assert len(cells)==1
    cell=cells[0];cell.text=None
    for child in list(cell):cell.remove(child)
    cell.text='2025'+suffix
    if suffix=='*':
        parsed=tables.parse_table(table,issuer='Nebius Group N.V.',filed=date(2026,8,12))
        assert parsed['kind']=='balance'
        assert {c['end'] for c in parsed['columns']}=={'2025-12-31','2026-06-30'}
        assert any('2025*' in c['header'] for c in parsed['columns'])
    else:
        with pytest.raises(tables.TabularFinancialError,match='date_header_unproved'):
            tables.parse_table(table,issuer='Nebius Group N.V.',filed=date(2026,8,12))


@pytest.mark.parametrize('mode',['disabled','api','public','pathless'])
def test_reader_flag_and_request_guard_prevent_database_and_source_work(monkeypatch,mode):
    from app import request_priority
    monkeypatch.setenv('SEC_FINANCIAL_DISCOVERY_READ_ENABLED','0' if mode=='disabled' else '1')
    context={'api':{'path':'/api/ticker/NBIS'},'public':{'path':'/public/ticker/NBIS'},'pathless':{'method':'GET'}}.get(mode)
    monkeypatch.setattr(request_priority,'get_request_context',lambda:context)
    monkeypatch.setattr(store,'read',lambda *a:pytest.fail('No checkpoint query expected'))
    client=Client()
    assert reader.prepared_choice(source(COMPANY),source(FACTS),symbol='NBIS',cik='0001513845',
        available_by=DAY,observed_at=NOW,client=client) is None
    assert not client.calls
