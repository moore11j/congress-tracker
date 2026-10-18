"""Complete SEC originals, ownership boundaries and accounting declarations."""
from datetime import date, datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
import gzip, hashlib, json, socket
import pytest
from lxml import html
from app.services import sec_tabular_financials as tables, sec_tabular_reporting as reporting
from app.services import sec_inline_financials as inline, sec_financial_statements as statements, sec_fundamentals as core
from test_sec_tabular_financials import simple

ROOT=Path(__file__).parent/'fixtures/sec_document_identity'
NOW=datetime(2026,10,18,4,50,tzinfo=timezone.utc)
CIK='0001703399'
CIKS={'SE':CIK,'YI':'0001738906'}


def originals(symbol="SE"):
    cik=CIKS[symbol]
    urls={}
    for url,m in json.loads((ROOT/'sources.json').read_text(encoding='utf-8')).items():
        raw=gzip.decompress((ROOT/m['file']).read_bytes())
        assert len(raw)==m['bytes'] and hashlib.sha256(raw).hexdigest()==m['sha256']
        urls[url]=raw
    return urls[f'https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json'],urls[f'https://data.sec.gov/submissions/CIK{cik}.json'],urls


class Client:
    issuer_document_urls=frozenset()
    def __init__(self,urls):self.urls=urls;self.calls=[]
    def get(self,url):self.calls.append(url);return self.urls[url]


@pytest.mark.parametrize('phrase',[
    'Our financial statements are prepared and presented in accordance with U.S. GAAP.',
    'Our financial information is presented in accordance with generally accepted accounting principles in the United States of America.',
    'Our financial performance is prepared in accordance with accounting principles generally accepted in the United States.',
])
def test_explicit_accounting_declarations(phrase):
    proof=reporting.declaration(('<p>'+phrase+'</p>').encode(),{'facts':[{'currency':'USD'}]})
    assert proof['accounting_basis']=='US GAAP'


@pytest.mark.parametrize('phrase',[
    'Our financial statements are not prepared and presented in accordance with U.S. GAAP.',
    'Our financial information is never presented in accordance with generally accepted accounting principles in the United States of America.',
    'Our financial statements are prepared and presented in accordance with GAAP.',
    'Our financial statements are prepared and presented in accordance with IFRS.',
    '<blockquote>Our financial statements are prepared and presented in accordance with U.S. GAAP.</blockquote>',
])
def test_unproved_accounting_declarations(phrase):
    with pytest.raises(reporting.TabularReportingError,match='accounting_basis_unproved'):
        reporting.declaration(('<html>'+phrase+'</html>').encode(),{'facts':[{'currency':'USD'}]})


def test_expanded_ifrs_conflict_remains_held():
    raw=b'<p>Our financial statements are prepared and presented in accordance with U.S. GAAP.</p><p>Our financial information is presented in accordance with IFRS.</p>'
    with pytest.raises(reporting.TabularReportingError,match='accounting_basis_conflict'):
        reporting.declaration(raw,{'facts':[{'currency':'USD'}]})


def unnamed(*,title='Example Corporation Reports Second Quarter 2026 Results',before='',heading='UNAUDITED CONSOLIDATED STATEMENTS OF OPERATIONS'):
    body=html.tostring(simple(heading=heading),encoding='unicode')
    text=('<html><body><p>'+title+'</p>'+before+
          '<p>'+'Background narrative. '*15+'</p>'+''.join('<p>'+str(n)+'</p>' for n in range(10))+
          '<p>'+heading+'</p><p>In thousands of U.S. dollars</p>'+body+'</body></html>')
    tree=html.fromstring(text)
    return tree,tree.xpath('//table')[0]


def test_document_headline_can_own_unnamed_statement_with_page_number():
    tree,table=unnamed();scope=tables.document_statement_scope(tree,'Example Corp')
    assert scope and scope['headline'].startswith('Example Corporation Reports')
    # Only the neutral page number directly precedes the local statement title.
    result=tables.parse_table(table,issuer='Example Corp',filed=date(2026,9,1),document_scope=scope)
    assert result['document_scope']==scope and result['observations'][0]['value']==1250000


@pytest.mark.parametrize('title',[
    'Different Corporation Reports Second Quarter 2026 Results',
    'Example Corporation is mentioned in this article',
    '<blockquote>Example Corporation Reports Second Quarter 2026 Results</blockquote>',
])
def test_wrong_incidental_or_quoted_company_is_not_document_identity(title):
    tree,table=unnamed(title=title)
    assert tables.document_statement_scope(tree,'Example Corp') is None


@pytest.mark.parametrize('caption',[
    'Acquired Other Corporation',
    'Example Subsidiary Corporation',
    'Other Corporation UNAUDITED CONSOLIDATED STATEMENTS OF OPERATIONS',
])
def test_competing_caption_cannot_be_hidden_by_page_numbers(caption):
    tree,table=unnamed();parent=table.getparent()
    for text in [caption]+[str(i) for i in range(12)]:
        node=html.Element('p');node.text=text;parent.insert(list(parent).index(table)-2,node)
    assert tables.document_statement_scope(tree,'Example Corp') is None


def test_complete_original_source_periods_cache_and_repeat():
    facts,company,urls=originals();client=Client(urls)
    with patch.object(socket.socket,'connect',side_effect=AssertionError('No network')):
        supplement=inline.acquire(facts,company,symbol='SE',cik=CIK,observed_at=NOW,client=client)
        assert len(client.calls)==len(set(client.calls))==6
        proof=supplement['evidence']['table_reporting']
        assert proof['native_observation_count']==18 and proof['currency']=='USD'
        assert proof['source']['statement_ownership'] and not proof['conflicts'] and not proof['holds']
        checked=reporting.validate(supplement,company_raw=company,symbol='SE',cik=CIK,available_by=date(2026,10,17))
        assert checked['records']['current_assets'][None,date(2026,6,30)]['value']==24052794000
        assert checked['records']['current_liabilities'][None,date(2026,6,30)]['value']==16094373000
        args=dict(symbol='SE',cik=CIK,observed_at=NOW,filing_supplement=supplement)
        panel=statements.project(facts,company,**args)
        assert panel==statements.project(facts,company,**args)
        assert panel['status']=='partial' and panel['reportingCurrency']=='USD'
        assert not panel['annual'] and not panel['quarterly']
        assert [r['revenue'] for r in panel['semiannual']]==[10100577000,14885268000]
        assert all(r['periodStart'].endswith('-01-01') and r['date'].endswith('-06-30') for r in panel['semiannual'])
        assert all(r['netIncome'] is None and r['eps'] is None and r['freeCashFlow'] is None for r in panel['semiannual'])
        assert panel['summary']['revenueTtm'] is None and panel['summary']['currentRatio']==24052794000/16094373000
        assert core.project_fundamentals(facts,company,**args)['current_ratio']==panel['summary']['currentRatio']
        row=SimpleNamespace(source='sec_edgar',fetched_at=NOW,payload_json=json.dumps(panel))
        assert statements.cached_row_payload(row,'SE',now=NOW)==panel
        assert not checked['records']['net_income'] and not checked['records']['equity']


def test_original_owner_or_ownership_proof_cannot_be_replaced():
    facts,company,urls=originals()
    supplement=inline.acquire(facts,company,symbol='SE',cik=CIK,observed_at=NOW,client=Client(urls))
    raw=supplement['table_original'];args=supplement['table_args']
    assert b'Sea Limited Reports' in raw
    with pytest.raises(reporting.TabularReportingError):
        reporting.normalize(raw.replace(b'Sea Limited Reports',b'Other Company Reports'),**args)
    supplement['evidence']['table_reporting']['source']['statement_ownership'][0]['issuer']='Other Company'
    with pytest.raises(reporting.TabularReportingError,match='evidence_changed'):
        reporting.validate(supplement,company_raw=company,symbol='SE',cik=CIK,available_by=date(2026,10,17))


@pytest.mark.parametrize('symbol,ratio',[('SE',24052794000/16094373000),('YI',1723085000/1605250000)])
def test_source_score_watchlist_transition_is_idempotent(symbol,ratio,monkeypatch):
    import test_sec_tagged_foreign_consumers as consumer
    def inputs(symbol):
        facts,company,urls=originals(symbol);return facts,company,CIKS[symbol],urls
    monkeypatch.setattr(consumer,'originals',inputs)
    monkeypatch.setattr(consumer,'EXPECTED',{symbol:{'metrics':{'current_ratio':ratio}}})
    monkeypatch.setattr(consumer,'NOW',NOW)
    consumer.test_original_source_score_and_alert_recovery_repeat_crossing_rollback(symbol,'current_ratio',monkeypatch)


def test_other_explicit_local_owner_beats_document_scope():
    tree,table=unnamed()
    scope=tables.document_statement_scope(tree,'Example Corp')
    p=html.Element('p');p.text='Other Corporation'
    table.getparent().insert(list(table.getparent()).index(table)-2,p)
    with pytest.raises(tables.TabularFinancialError,match='table_issuer_unproved'):
        tables.parse_table(table,issuer='Example Corp',filed=date(2026,9,1),document_scope=scope)


def test_unit_caption_cannot_hide_other_issuer_behind_page_markers():
    tree,table=unnamed();parent=table.getparent()
    for text in ['Other Corporation','In thousands of USD']+[str(i) for i in range(12)]:
        node=html.Element('p');node.text=text;parent.insert(list(parent).index(table)-2,node)
    assert tables.document_statement_scope(tree,'Example Corp') is None


def test_complete_yi_source_preserves_native_quarters_and_convenience_columns():
    facts,company,urls=originals('YI');client=Client(urls);cik=CIKS['YI']
    with patch.object(socket.socket,'connect',side_effect=AssertionError('No network')):
        supplement=inline.acquire(facts,company,symbol='YI',cik=cik,observed_at=NOW,client=client)
        assert len(client.calls)==len(set(client.calls))==4
        proof=supplement['evidence']['table_reporting']
        assert proof['native_observation_count']==24 and proof['source_observation_count']==36
        assert proof['currency']=='CNY'
        args=dict(symbol='YI',cik=cik,observed_at=NOW,filing_supplement=supplement)
        panel=statements.project(facts,company,**args)
        assert panel==statements.project(facts,company,**args)
        assert panel['reportingCurrency']=='CNY' and panel['status']=='partial'
        assert [r['revenue'] for r in panel['quarterly']]==[3205760000,2300117000]
        assert [r['operatingCashFlow'] for r in panel['quarterly']]==[-61410000,-11436000]
        assert all(r['periodStart'].endswith('-04-01') and r['netIncome'] is None and r['eps'] is None for r in panel['quarterly'])
        assert panel['summary']['currentRatio']==1723085000/1605250000
        assert core.project_fundamentals(facts,company,**args)['current_ratio']==panel['summary']['currentRatio']
        assert panel['summary']['revenueTtm'] is None


@pytest.mark.parametrize('symbol,ratio',[('SE',24052794000/16094373000),('YI',1723085000/1605250000)])
def test_preparation_cache_repeat_does_not_refetch(symbol,ratio,monkeypatch):
    from sqlalchemy import create_engine,select,func
    from sqlalchemy.orm import sessionmaker
    from app.models import InsightsSnapshot
    from app.services import sec_fundamentals_preparation as prep,sec_directory
    facts,company,urls=originals(symbol);client=Client(urls);cik=CIKS[symbol]
    monkeypatch.setattr(socket.socket,'connect',lambda *a,**k:pytest.fail('Unexpected network'))
    monkeypatch.setenv('SEC_FUNDAMENTALS_WARMING_ENABLED','1')
    monkeypatch.setattr(sec_directory,'directory',lambda:{symbol:{'cik':cik}})
    class Clock(datetime):
        @classmethod
        def now(cls,tz=None):return NOW
    engine=create_engine('sqlite://');InsightsSnapshot.__table__.create(engine);factory=sessionmaker(bind=engine)
    with patch.object(prep,'SessionLocal',factory),patch.object(prep,'datetime',Clock),patch.object(prep,'DirectSourceClient',lambda:client):
        payload=prep.prepare(symbol);assert payload['values']=={'current_ratio':ratio}
        count=len(client.calls);assert prep.prepare(symbol)==payload and len(client.calls)==count
    with patch.object(statements,'SessionLocal',factory),patch.object(statements,'datetime',Clock),patch.object(statements,'DirectSourceClient',lambda:client):
        panel=statements.prepared(symbol);assert panel['status']=='partial'
        count=len(client.calls);assert statements.prepared(symbol)==panel and len(client.calls)==count
    with factory() as db:assert db.scalar(select(func.count()).select_from(InsightsSnapshot))==2
    engine.dispose()
