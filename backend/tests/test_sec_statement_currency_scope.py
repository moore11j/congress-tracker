"""Complete original DOM/currency evidence and publication boundaries."""
from datetime import date,datetime,timezone
from pathlib import Path
from unittest.mock import patch
import gzip,hashlib,json,socket
import pytest
from lxml import html
from app.services import sec_tabular_financials as tables,sec_tabular_reporting as reporting
from app.services import sec_inline_financials as inline,sec_financial_statements as statements,sec_fundamentals as core,sec_document_sets as sets
from test_sec_tabular_financials import simple

ROOT=Path(__file__).parent/'fixtures/sec_statement_currency_scope'
NOW=datetime(2026,10,18,5,40,tzinfo=timezone.utc)
CIKS={'DOX':'0001062579','ASC':'0001577437'}
RATIO=1592765000/1702496000

def originals(symbol):
    urls={};cik=CIKS[symbol]
    for u,m in json.loads((ROOT/'sources.json').read_text(encoding='utf-8')).items():
        raw=gzip.decompress((ROOT/m['file']).read_bytes())
        assert len(raw)==m['bytes'] and hashlib.sha256(raw).hexdigest()==m['sha256'];urls[u]=raw
    return urls[f'https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json'],urls[f'https://data.sec.gov/submissions/CIK{cik}.json'],cik,urls

class Client:
    issuer_document_urls=frozenset()
    def __init__(self,urls):self.urls=urls;self.calls=[]
    def get(self,url):self.calls.append(url);return self.urls[url]

def acquired():
    facts,company,cik,urls=originals('DOX');client=Client(urls)
    supplement=inline.acquire(facts,company,symbol='DOX',cik=cik,observed_at=NOW,client=client)
    return facts,company,cik,urls,client,supplement


def test_complete_current_primary_and_proven_earlier_release_periods():
    with patch.object(socket.socket,'connect',side_effect=AssertionError('No network')):
        facts,company,cik,urls,client,supplement=acquired()
        assert len(client.calls)==len(set(client.calls))==5
        proof=supplement['evidence']['table_reporting'];source=proof['source']
        assert proof['native_observation_count']==28
        assert source['filed']=='2026-08-18' and proof['current_period']=='2026-06-30'
        assert source['statement_sha256']==hashlib.sha256(supplement['table_original']).hexdigest()
        assert source['document_role']=='primary_filing' and len(supplement['table_original'])>2_000_000
        assert source['statement_currency'] and all(p['issuer']=='AMDOCS LTD' and p['currency']=='USD' for p in source['statement_currency'])
        holds=proof['discovery']['holds'];assert len(holds)==2
        assert all(h['reason']=='table_accounting_basis_unproved' for h in holds)
        relevant=[h for h in holds if h['filed']>proof['current_period']];assert len(relevant)==1
        assert all(not h['period_evidence']['financial_values_qualified'] for h in relevant)
        assert all(h['period_evidence']['end']<=proof['current_period'] and h['filed']<source['filed'] for h in relevant)
        args=dict(symbol='DOX',cik=cik,observed_at=NOW,filing_supplement=supplement)
        panel=statements.project(facts,company,**args);assert panel==statements.project(facts,company,**args)
        assert panel['status']=='partial' and panel['annual']==[]
        assert [r['revenue'] for r in panel['quarterly']]==[1144437000,1174895000]
        assert [r['netIncome'] for r in panel['quarterly']]==[154001000,62169000]
        assert all(r['eps'] is None and r['freeCashFlow'] is None and r['operatingCashFlow'] is None for r in panel['quarterly'])
        assert panel['summary']['revenueTtm'] is None and panel['summary']['currentRatio']==RATIO
        assert core.project_fundamentals(facts,company,**args)['current_ratio']==RATIO


def test_shared_currency_header_matches_independent_tagged_statement():
    facts,company,cik,urls=originals('ASC');u=next(u for u in urls if u.endswith('asc-20260630x6k.htm'));raw=urls[u]
    extracted=tables.extract(raw,source_url=u,company_raw=company,cover_raw=raw,cover_url=u,symbol='ASC',cik=cik,available_by=date(2026,10,17))
    revenue=next(f for f in extracted['facts'] if f['metric']=='revenue' and f['start']=='2026-04-01' and f['end']=='2026-06-30')
    assert revenue['currency']=='USD' and revenue['value']==116214000
    assert revenue['occurrences'][0]['label']=='Revenue, net'
    supplement=inline.acquire(facts,company,symbol='ASC',cik=cik,observed_at=NOW,client=Client(urls))
    panel=statements.project(facts,company,symbol='ASC',cik=cik,observed_at=NOW,filing_supplement=supplement)
    assert panel['quarterly'][-1]['revenue']==revenue['value']
    assert len(panel['annual'])==6 and len(panel['quarterly'])==12


@pytest.mark.parametrize('source,expected',[
 ('<p>CONSOLI<span>DATED</span> STATEMEN<b>TS</b> OF INCOME</p>','CONSOLIDATED STATEMENTS OF INCOME'),
 ('<p>U.S. dol<span>lars</span></p>','U.S. dollars'),
 ('<p>one<br/>two</p>','one two'),
 ('<div><p>one</p><p>two</p></div>','one two'),
 ('<p>Revenue <b>net</b></p>','Revenue net'),
 ('<p>1<span>,234</span></p>','1,234'),
])
def test_rendered_inline_words_and_block_boundaries(source,expected):
    assert tables.visible_text(html.fromstring(source))==expected


def document(phrase,extra=''):
    return html.fromstring('<div><p>Example Corp</p><p>Consolidated Balance Sheets</p><p>'+phrase+'</p>'+extra+'</div>')

@pytest.mark.parametrize('phrase',[
 'The consolidated financial statements are presented in U.S. dollars.',
 'The unaudited consolidated financial statements of the Company have been prepared in accordance with U.S. generally accepted accounting principles, or GAAP and are denominated in U.S. dollars.',
 'The interim financial statements included in this Report have been prepared in accordance with U.S. GAAP and are presented in USD.',
])
def test_explicit_statement_currency(phrase):
    proof=tables.statement_currency_scope(document(phrase),'Example Corp')
    assert proof['currency']=='USD' and proof['issuer']=='Example Corp'
    assert proof['declarations'] and 'financial statements' in proof['declarations'][0]['declaration']

@pytest.mark.parametrize('phrase',[
 'Notional values of derivatives are denominated in U.S. dollars.',
 'The consolidated financial statements are presented in dollars.',
 'The financial statements of Other Company are presented in USD.',
 'The financial statements are not presented in USD.',
 'If the financial statements are presented in USD, these examples apply.',
 'The financial statements have been prepared in accordance with U.S. GAAP and are not denominated in USD.',
])
def test_unqualified_currency_stays_unavailable(phrase):
    assert tables.statement_currency_scope(document(phrase),'Example Corp') is None

@pytest.mark.parametrize('extra',[
 '<p>The financial statements are presented in Canadian dollars.</p>',
 '<p>Other Company Consolidated Statements of Income</p>',
 '<table><tr><td><p>Other Company Consolidated Statements of Income</p></td></tr></table>',
])
def test_competing_currency_or_owner_disables_scope(extra):
    assert tables.statement_currency_scope(document('The financial statements are presented in USD.',extra),'Example Corp') is None


def test_quoted_currency_is_not_a_declaration():
    assert tables.statement_currency_scope(document('No currency declared.','<blockquote><p>The financial statements are presented in USD.</p></blockquote>'),'Example Corp') is None


def test_local_currency_cannot_conflict_with_statement_scope():
    proof=tables.statement_currency_scope(document('The financial statements are presented in Canadian dollars.'),'Example Corp')
    with pytest.raises(tables.TabularFinancialError,match='currency_conflict'):
        tables.parse_table(simple(),issuer='Example Corp',filed=date(2026,9,1),currency_scope=proof)


def test_newer_period_held_release_is_not_hidden_by_later_filing():
    facts,company,cik,urls,client,supplement=acquired()
    u=next(u for u in urls if u.endswith('d29115dex991.htm'));raw=urls[u]
    urls[u]=raw.replace(b'June&nbsp;30',b'July&nbsp;31');assert urls[u]!=raw
    with pytest.raises(reporting.TabularReportingError,match='discovery_has_unqualified_statement'):
        inline.acquire(facts,company,symbol='DOX',cik=cik,observed_at=NOW,client=Client(urls))


def test_later_unqualified_amendment_is_not_hidden_by_primary():
    facts,company,cik,urls,client,supplement=acquired();co=json.loads(company);recent=co['filings']['recent']
    i=recent['accessionNumber'].index('0001193125-26-336043');recent['filingDate'][i]='2026-09-01';company=json.dumps(co).encode()
    with pytest.raises(reporting.TabularReportingError,match='discovery_has_unqualified_statement'):
        inline.acquire(facts,company,symbol='DOX',cik=cik,observed_at=NOW,client=Client(urls))


def test_malformed_owned_period_cannot_supersede_hold():
    facts,company,cik,urls,client,supplement=acquired();u=next(u for u in urls if u.endswith('d29115dex991.htm'))
    raw=urls[u].replace(b'June&nbsp;30',b'June&nbsp;35')
    assert tables.inspect_statement_period(raw,issuer='AMDOCS LTD',filed=date(2026,8,5)) is None


def test_original_currency_proof_cannot_be_edited():
    facts,company,cik,urls,client,supplement=acquired()
    supplement['evidence']['table_reporting']['source']['statement_currency'][0]['currency']='CAD'
    with pytest.raises(reporting.TabularReportingError,match='evidence_changed'):
        reporting.validate(supplement,company_raw=company,symbol='DOX',cik=cik,available_by=date(2026,10,17))


def test_discovery_keeps_twenty_megabyte_original_bound():
    with pytest.raises(inline.InlineFactsError,match='cover_size'):
        sets.statement_exhibits(b'x'*20_000_001,{})


def test_source_score_watchlist_recovery_crossing_repeat_rollback(monkeypatch):
    import test_sec_tagged_foreign_consumers as consumer
    monkeypatch.setattr(consumer,'originals',originals);monkeypatch.setattr(consumer,'EXPECTED',{'DOX':{'metrics':{'current_ratio':RATIO}}});monkeypatch.setattr(consumer,'NOW',NOW)
    consumer.test_original_source_score_and_alert_recovery_repeat_crossing_rollback('DOX','current_ratio',monkeypatch)


def test_source_cache_repeat_does_not_refetch(monkeypatch):
    import test_sec_document_identity as cache
    def inputs(symbol):
        facts,company,cik,urls=originals(symbol);return facts,company,urls
    monkeypatch.setattr(cache,'originals',inputs);monkeypatch.setattr(cache,'CIKS',CIKS);monkeypatch.setattr(cache,'NOW',NOW)
    cache.test_preparation_cache_repeat_does_not_refetch('DOX',RATIO,monkeypatch)


def test_failed_only_scopes_do_not_reparse_periods(monkeypatch):
    facts,company,cik,urls=originals('DOX')
    def unqualified(*args,**kwargs):raise reporting.TabularReportingError('table_accounting_basis_unproved')
    monkeypatch.setattr(reporting,'normalize',unqualified)
    monkeypatch.setattr(tables,'inspect_statement_period',lambda *a,**k:pytest.fail('Unnecessary period pass'))
    assert reporting.acquire(company,symbol='DOX',cik=cik,available_by=date(2026,10,17),client=Client(urls)) is None
