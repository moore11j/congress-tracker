"""Complete domestic cover/exhibit sources and unchanged integrity boundaries."""
from pathlib import Path
from copy import deepcopy
from datetime import datetime,timezone
from functools import lru_cache
import gzip,hashlib,json,socket
import pytest
from app.clients.direct_sources import DirectSourceError
from app.services import sec_document_sets as sets,sec_inline_financials as inline
from app.services import sec_fundamentals as core,sec_financial_statements as panels,sec_tagged_foreign as tagged

ROOT=Path(__file__).parent/'fixtures/sec_domestic_document_sets'
NOW=datetime(2026,10,31,23,50,tzinfo=timezone.utc)
CIK='0000021076'
COVER='https://www.sec.gov/Archives/edgar/data/21076/000002107626000034/clx-20260630.htm'
EXHIBIT=COVER.replace('.htm','_d2.htm')

@lru_cache
def sources():
 manifest=json.loads((ROOT/'sources.json').read_text());out={}
 for name,m in manifest.items():
  raw=gzip.decompress((ROOT/m['file']).read_bytes())
  assert len(raw)==m['bytes'] and hashlib.sha256(raw).hexdigest()==m['sha256']
  out[m['url']]=raw
 return out

class Client:
 def __init__(self,data=None):self.calls=[];self.data=data or sources()
 def get(self,url):
  self.calls.append(url);assert url in self.data,'Unretained original: '+url
  return self.data[url]

def company():return sources()[f'https://data.sec.gov/submissions/CIK{CIK}.json']
def facts():return sources()[f'https://data.sec.gov/api/xbrl/companyfacts/CIK{CIK}.json']
def metadata():return dict(cik=CIK,accession='0000021076-26-000034',form='10-K',filed='2026-08-07',report_date='2026-06-30',cover_url=COVER)
def acquire(client=None):
 return inline.acquire(facts(),company(),symbol='CLX',cik=CIK,observed_at=NOW,client=client or Client(),include_debt_precision=True)

def test_actual_complete_acquisition_and_full_projection_repeat(monkeypatch):
 monkeypatch.setattr(socket.socket,'connect',lambda *a,**k:pytest.fail('Network forbidden'))
 client=Client();supplement=acquire(client)
 assert len(client.calls)==len(set(client.calls))==2
 assert set(client.calls)=={COVER,EXHIBIT}
 assert supplement==acquire()
 proof=supplement['evidence'];assert sets.domestic_cached_valid(proof)
 assert proof['sha256']==hashlib.sha256(sources()[EXHIBIT]).hexdigest()
 assert proof['cover_evidence']['source_sha256']==hashlib.sha256(sources()[COVER]).hexdigest()
 assert all(p['source_url']==EXHIBIT and p['source_sha256']==proof['sha256'] for p in supplement['provenance'].values())
 args=dict(symbol='CLX',cik=CIK,observed_at=NOW,filing_supplement=supplement)
 value=core.project_fundamentals(facts(),company(),**args)
 assert value==core.project_fundamentals(facts(),company(),**args)
 expected={'gross_margin':42.32142857142857,'net_margin':8.735119047619047,'revenue_growth':-5.4054054054054035,
  'current_ratio':0.6577713667508114,'roe':285.6447688564477,'free_cash_flow':405000000.0,
  'fcf_margin':6.026785714285714,'fcf_growth':-46.78055190538765}
 assert {k:v for k,v in value.items() if type(v) in (int,float)}==expected
 assert 'operating_margin_expansion' not in value and 'net_debt_to_ebitda' not in value
 evidence=json.loads(value['source_evidence_json']);assert tagged.cached_currency_valid(evidence)
 panel=panels.project(facts(),company(),**args);assert panel==panels.project(facts(),company(),**args)
 assert panel['status']=='partial' and panel['annual']
 assert panel['sourceEvidence']['inlineFilingSupplement']['domestic_linked_statement']==proof['domestic_linked_statement']

@pytest.fixture(scope='module')
def parsed():return acquire()

@pytest.mark.parametrize('field',['policy','directory_sha256','directory_url','statement_url','statement_label','cover_url','cik','accession','form','filed','report_date'])
def test_corrupt_cached_directory_binding_is_held(parsed,field):
 proof=deepcopy(parsed['evidence']);proof['domestic_linked_statement'][field]='invalid'
 assert not sets.domestic_cached_valid(proof)
 assert not tagged.cached_currency_valid({'inline_filing_supplement':proof})

@pytest.mark.parametrize('field',['source_sha256','exhibit_sha256','source_url','exhibit_url','methodology'])
def test_corrupt_cached_original_binding_is_held(parsed,field):
 proof=deepcopy(parsed['evidence']);proof['cover_evidence'][field]='invalid'
 assert not sets.domestic_cached_valid(proof)

def test_stripped_domestic_proof_is_not_an_ordinary_cache(parsed):
 proof=deepcopy(parsed['evidence']);del proof['domestic_linked_statement']
 assert not tagged.cached_currency_valid({'inline_filing_supplement':proof})

@pytest.mark.parametrize('defect',['no_link','external_link','ambiguous_link','wrong_symbol','wrong_directory','wrong_form','wrong_date','cover_financial_fact','duplicate_context','namespace_conflict'])
def test_domestic_original_validation_and_source_scope(defect):
 data=dict(sources());cover=data[COVER];exhibit=data[EXHIBIT];meta=metadata();raw_company=company();symbol='CLX'
 if defect=='no_link':cover=cover.replace(b'clx-20260630_d2.htm',b'no-statement.htm')
 elif defect=='external_link':cover=cover.replace(b'clx-20260630_d2.htm',b'https://example.com/statement.htm')
 elif defect=='ambiguous_link':cover=cover.replace(b'</body>',b'<a href="second.htm">Financial statements</a></body>')
 elif defect=='wrong_symbol':symbol='OTHER'
 elif defect=='wrong_directory':
  payload=json.loads(raw_company);idx=payload['filings']['recent']['accessionNumber'].index(meta['accession']);payload['filings']['recent']['primaryDocument'][idx]='different.htm';raw_company=json.dumps(payload).encode()
 elif defect=='wrong_form':meta['form']='10-Q'
 elif defect=='wrong_date':meta['report_date']='2026-03-31'
 elif defect=='cover_financial_fact':cover=cover.replace(b'</body>',b'<ix:nonFraction name="us-gaap:Revenue" contextRef="c-1" unitRef="usd">5</ix:nonFraction></body>')
 elif defect=='duplicate_context':exhibit=exhibit.replace(b'</body>',b'<xbrli:context id="c-1"></xbrli:context></body>')
 elif defect=='namespace_conflict':exhibit=exhibit.replace(b'http://www.xbrl.org/2013/inlineXBRL',b'https://example.com/inlineXBRL')
 data[COVER]=cover;data[EXHIBIT]=exhibit;client=Client(data)
 with pytest.raises((ValueError,AssertionError,DirectSourceError)):
  sets.acquire_domestic(raw_company,symbol=symbol,metadata=meta,cover=cover,client=client,available_by=NOW.date())
 assert all(u.startswith(COVER.rsplit('/',1)[0]+'/') for u in client.calls)
 assert len(client.calls)<=1

def test_existing_non_coverage_failure_is_not_retried(monkeypatch):
 calls=[]
 def rejected(*args,**kwargs):calls.append(1);raise inline.InlineFactsError('conflicting_reported_facts')
 monkeypatch.setattr(inline,'parse',rejected);client=Client()
 with pytest.raises(inline.InlineFactsError,match='conflicting_reported_facts'):
  sets.acquire_domestic(company(),symbol='CLX',metadata=metadata(),cover=sources()[COVER],client=client,available_by=NOW.date())
 assert len(calls)==1 and not client.calls


@pytest.mark.parametrize('metric',['roe','revenue_growth'])
def test_original_source_score_watchlist_rebaseline_crossing_and_rollback(metric,monkeypatch):
 import test_sec_tagged_foreign_consumers as consumer
 value=core.project_fundamentals(facts(),company(),symbol='CLX',cik=CIK,observed_at=NOW,filing_supplement=acquire())
 metrics={k:v for k,v in value.items() if type(v) in (int,float)}
 monkeypatch.setattr(consumer,'originals',lambda symbol:(facts(),company(),CIK,sources()))
 monkeypatch.setattr(consumer,'EXPECTED',{'CLX':{'metrics':metrics}})
 monkeypatch.setattr(consumer,'NOW',NOW)
 consumer.test_original_source_score_and_alert_recovery_repeat_crossing_rollback('CLX',metric,monkeypatch)
