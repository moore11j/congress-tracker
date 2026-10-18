"""Complete primary-file chains, ownership, discovery and consumer boundaries."""
from datetime import date,datetime,timezone
from pathlib import Path
from unittest.mock import patch
import gzip,hashlib,json,socket
import pytest
from lxml import html
from app.services import sec_tabular_reporting as reporting,sec_tabular_financials as tables,sec_document_sets as sets
from app.services import sec_inline_financials as inline,sec_financial_statements as statements,sec_fundamentals as core
from test_sec_tabular_financials import simple

ROOT=Path(__file__).parent/'fixtures/sec_primary_statements'
NOW=datetime(2026,10,18,5,20,tzinfo=timezone.utc)
CASES={
 'TK':{'cik':'0000911971','section':'semiannual','revenue':[463333000,664935000],'assets':1524874000,'liabilities':147174000,'count':16},
 'TNK':{'cik':'0001419945','section':'quarterly','revenue':[232866000,379508000],'assets':1470123000,'liabilities':147219000,'count':22},
}


def originals(symbol):
    urls={};cik=CASES[symbol]['cik']
    for u,m in json.loads((ROOT/'sources.json').read_text(encoding='utf-8')).items():
        raw=gzip.decompress((ROOT/m['file']).read_bytes())
        assert len(raw)==m['bytes'] and hashlib.sha256(raw).hexdigest()==m['sha256'];urls[u]=raw
    return urls[f'https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json'],urls[f'https://data.sec.gov/submissions/CIK{cik}.json'],cik,urls


class Client:
    issuer_document_urls=frozenset()
    def __init__(self,urls):self.urls=urls;self.calls=[]
    def get(self,url):self.calls.append(url);return self.urls[url]


def acquired(symbol):
    facts,company,cik,urls=originals(symbol);client=Client(urls)
    supplement=inline.acquire(facts,company,symbol=symbol,cik=cik,observed_at=NOW,client=client)
    return facts,company,cik,urls,client,supplement


@pytest.mark.parametrize('symbol',CASES)
def test_complete_primary_source_and_unchanged_request_bound(symbol):
    w=CASES[symbol]
    with patch.object(socket.socket,'connect',side_effect=AssertionError('No network')):
        facts,company,cik,urls,client,supplement=acquired(symbol)
        assert len(client.calls)==len(set(client.calls))==3
        proof=supplement['evidence']['table_reporting'];source=proof['source']
        assert source['document_role']=='primary_filing' and source['statement_url']==source['cover_url']
        assert source['statement_sha256']==source['cover_sha256']==hashlib.sha256(supplement['table_original']).hexdigest()
        assert proof['native_observation_count']==w['count'] and proof['currency']=='USD'
        assert source['filed']=='2026-07-31' and proof['current_period']=='2026-06-30'
        checked=reporting.validate(supplement,company_raw=company,symbol=symbol,cik=cik,available_by=date(2026,10,17))
        assert checked['records']['current_assets'][None,date(2026,6,30)]['value']==w['assets']
        assert checked['records']['current_liabilities'][None,date(2026,6,30)]['value']==w['liabilities']
        args=dict(symbol=symbol,cik=cik,observed_at=NOW,filing_supplement=supplement)
        panel=statements.project(facts,company,**args)
        assert panel==statements.project(facts,company,**args)
        assert panel['status']=='partial' and panel['reportingCurrency']=='USD'
        assert [r['revenue'] for r in panel[w['section']]]==w['revenue']
        assert all(r['netIncome'] is None and r['eps'] is None and r['freeCashFlow'] is None for r in panel[w['section']])
        assert panel['summary']['revenueTtm'] is None
        assert panel['summary']['currentRatio']==w['assets']/w['liabilities']
        assert core.project_fundamentals(facts,company,**args)['current_ratio']==panel['summary']['currentRatio']
        if symbol=='TK':assert checked['records']['revenue'][date(2026,1,1),date(2026,6,30)]['source_occurrences'][0]['label']=='Revenues ( note 3 )'


@pytest.mark.parametrize('label',['Revenues (note 3)','Revenues ( note 3 )','Revenues (notes 3 and 5)','Revenues (notes 3, 5)','Revenues (notes 3-5)'])
def test_numeric_note_reference_preserves_line_item(label):
    assert tables.metric_for(label,'income','Example')=='revenue'


@pytest.mark.parametrize('label',['Revenues (adjusted)','Revenues (non-GAAP)','Revenues (per share)','Revenues (note revenue)','Revenues (USD)','Revenues (note 3) from discontinued operations'])
def test_other_parenthetical_meaning_is_never_removed(label):
    assert tables.metric_for(label,'income','Example') is None


def test_summary_statement_title_does_not_become_company_name():
    t=simple(heading='Summary Consolidated Statements of Operations')
    r=tables.parse_table(t,issuer='Example Corp',filed=date(2026,9,1))
    assert r['observations'][0]['value']==1250000
    with pytest.raises(tables.TabularFinancialError,match='issuer_unproved'):
        tables.parse_table(t,issuer='Other Company',filed=date(2026,9,1))
    assert tables.statement_prefix('Other Company Summary Consolidated Statements of Income')=='Other Company'
    assert tables.statement_prefix('Summary Corporation Consolidated Statements of Income')=='Summary Corporation'


@pytest.mark.parametrize('symbol',CASES)
def test_contents_anchors_are_not_external_sources(symbol):
    facts,company,cik,urls,client,supplement=acquired(symbol)
    args=supplement['table_args'];metadata=next(c for c in sets.candidates(company,symbol=symbol,cik=cik,available_by=date(2026,10,17)) if c['cover_url']==args['cover_url'])
    assert not sets.statement_exhibits(args['cover_raw'],metadata)
    raw=args['cover_raw'].replace(b'</body>',b'<a href="https://example.test/financial.htm">Financial Statements</a></body>')
    assert raw!=args['cover_raw']
    with pytest.raises(inline.InlineFactsError,match='statement_link_scope'):sets.statement_exhibits(raw,metadata)


def test_primary_requires_identical_source_bytes_and_directory_identity():
    facts,company,cik,urls,client,supplement=acquired('TK');args=dict(supplement['table_args']);raw=supplement['table_original']
    args['cover_raw']=raw+b' '
    with pytest.raises(tables.TabularFinancialError,match='primary_bytes_mismatch'):reporting.normalize(raw,**args)
    args=dict(supplement['table_args']);args['cover_url']=args['cover_url'].replace('tkinterimfsq2-26doc.htm','other.htm')
    with pytest.raises(tables.TabularFinancialError,match='directory_identity'):reporting.normalize(raw,**args)
    supplement['evidence']['table_reporting']['source']['document_role']='linked_exhibit'
    with pytest.raises(reporting.TabularReportingError,match='evidence_changed'):
        reporting.validate(supplement,company_raw=company,symbol='TK',cik=cik,available_by=date(2026,10,17))


def test_primary_and_exhibit_with_equal_selection_rank_are_held():
    facts,company,cik,urls,client,supplement=acquired('TK');args=supplement['table_args'];raw=supplement['table_original'];u=args['cover_url'];extra=u.rsplit('/',1)[0]+'/additional.htm'
    urls[u]=raw.replace(b'</body>',b'<a href="additional.htm">Financial Statements</a></body>');assert urls[u]!=raw
    urls[extra]=raw.replace(b'664,935',b'774,935');assert urls[extra]!=raw
    with pytest.raises(reporting.TabularReportingError,match='current_selection_ambiguous'):
        reporting.acquire(company,symbol='TK',cik=cik,available_by=date(2026,10,17),client=Client(urls))


def test_unqualified_newer_primary_blocks_older_fallback():
    facts,company,cik,urls,client,supplement=acquired('TK');co=json.loads(company);recent=co['filings']['recent']
    fields={'accessionNumber':'0000911971-26-999999','form':'6-K','filingDate':'2026-09-20','reportDate':'2026-09-20','primaryDocument':'held.htm'}
    for key,rows in recent.items():rows.insert(0,fields.get(key,''))
    company=json.dumps(co).encode();u='https://www.sec.gov/Archives/edgar/data/911971/000091197126999999/held.htm'
    raw=supplement['table_original'];urls[u]=raw.replace(b'United States generally accepted accounting principles',b'an unproved accounting basis');assert urls[u]!=raw
    with pytest.raises(reporting.TabularReportingError,match='discovery_has_unqualified_statement'):
        reporting.acquire(company,symbol='TK',cik=cik,available_by=date(2026,10,17),client=Client(urls))


@pytest.mark.parametrize('phrase',[
 'Our financial statements are prepared in accordance with United States generally accepted accounting principles.',
 'Our financial statements are prepared in accordance with U.S. generally accepted accounting principles.',
 'The most directly comparable financial measures under United States generally accepted accounting principles are shown in the statements.',
])
def test_explicit_us_standard_orderings(phrase):
    assert reporting.declaration(('<p>'+phrase+'</p>').encode(),{'facts':[{'currency':'USD'}]})['accounting_basis']=='US GAAP'


@pytest.mark.parametrize('phrase',[
 'Our financial statements are not prepared in accordance with United States generally accepted accounting principles.',
 'The most directly comparable financial measures under GAAP are shown in the statements.',
 'Our financial statements follow generally accepted accounting principles.',
 'These are not the most directly comparable financial measures under United States generally accepted accounting principles.',
 'Unlike the most directly comparable financial measures under U.S. GAAP, these numbers are adjusted.',
])
def test_missing_or_negated_us_accounting_declaration_stays_held(phrase):
    with pytest.raises(reporting.TabularReportingError,match='accounting_basis_unproved'):
        reporting.declaration(('<p>'+phrase+'</p>').encode(),{'facts':[{'currency':'USD'}]})


@pytest.mark.parametrize('symbol',CASES)
def test_source_score_watchlist_recovery_repeat_crossing_rollback(symbol,monkeypatch):
    import test_sec_tagged_foreign_consumers as consumer
    w=CASES[symbol]
    monkeypatch.setattr(consumer,'originals',originals)
    monkeypatch.setattr(consumer,'EXPECTED',{symbol:{'metrics':{'current_ratio':w['assets']/w['liabilities']}}})
    monkeypatch.setattr(consumer,'NOW',NOW)
    consumer.test_original_source_score_and_alert_recovery_repeat_crossing_rollback(symbol,'current_ratio',monkeypatch)


@pytest.mark.parametrize('symbol',CASES)
def test_prepared_source_caches_repeat_without_requests(symbol,monkeypatch):
    import test_sec_document_identity as cache
    w=CASES[symbol]
    def inputs(name):
        facts,company,cik,urls=originals(name);return facts,company,urls
    monkeypatch.setattr(cache,'originals',inputs)
    monkeypatch.setattr(cache,'CIKS',{symbol:w['cik']})
    monkeypatch.setattr(cache,'NOW',NOW)
    cache.test_preparation_cache_repeat_does_not_refetch(symbol,w['assets']/w['liabilities'],monkeypatch)
