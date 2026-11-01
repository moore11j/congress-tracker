"""Complete source and consumer proof for SEC duplicate-precision overlap."""
from pathlib import Path
from datetime import datetime,timezone
from functools import lru_cache
import gzip,hashlib,json,socket
import pytest
from app.services import sec_inline_financials as inline,sec_fundamentals as core,sec_financial_statements as panels

ROOT=Path(__file__).parent/'fixtures/sec_duplicate_overlap'
CIK='0001800315'
NOW=datetime(2026,11,1,0,10,tzinfo=timezone.utc)
URL='https://www.sec.gov/Archives/edgar/data/1800315/000119312526342307/dmra-20260630.htm'

@lru_cache
def originals():
 out={}
 for name,m in json.loads((ROOT/'sources.json').read_text()).items():
  raw=gzip.decompress((ROOT/m['file']).read_bytes())
  assert len(raw)==m['bytes'] and hashlib.sha256(raw).hexdigest()==m['sha256']
  out[m['url']]=raw
 return out

def facts():return originals()[f'https://data.sec.gov/api/xbrl/companyfacts/CIK{CIK}.json']
def company():return originals()[f'https://data.sec.gov/submissions/CIK{CIK}.json']
class Client:
 issuer_document_urls=frozenset()
 def __init__(self):self.calls=[]
 def get(self,url):self.calls.append(url);assert url in originals();return originals()[url]

def acquire(client=None):
 return inline.acquire(facts(),company(),symbol='DMRA',cik=CIK,observed_at=NOW,client=client or Client(),include_debt_precision=True)

@pytest.fixture(scope='module')
def parsed():return acquire()

def test_complete_source_recovers_finer_values_and_repeats(parsed,monkeypatch):
 monkeypatch.setattr(socket.socket,'connect',lambda *a,**k:pytest.fail('Network forbidden'))
 client=Client();assert acquire(client)==parsed and client.calls==[URL]
 assert parsed['evidence']['reported_facts']==34
 proof=parsed['evidence']['core_rounded_duplicates']
 assert proof['methodology']=='edgar_consistent_duplicates_v2'
 assert len(proof['comparisons'])==6
 key=('LiabilitiesCurrent','USD',None,'2026-06-30')
 assert parsed['provenance'][key]['fact_id']=='F_7795714b-457b-430d-8c6f-c96fc7438f1a'
 assert parsed['provenance'][key]['source_sha256']==hashlib.sha256(originals()[URL]).hexdigest()
 row=next(r for r in parsed['facts']['LiabilitiesCurrent']['units']['USD'] if r['end']=='2026-06-30')
 assert row['val']==27550000
 args=dict(symbol='DMRA',cik=CIK,observed_at=NOW,filing_supplement=parsed)
 value=core.project_fundamentals(facts(),company(),**args)
 assert value==core.project_fundamentals(facts(),company(),**args)
 assert {k:v for k,v in value.items() if type(v) in (int,float)}=={'current_ratio':19.730961887477314,'roe':-99.74194135996903}
 panel=panels.project(facts(),company(),**args)
 assert panel==panels.project(facts(),company(),**args)
 assert panel['status']=='unavailable'
 assert not panel['annual'] and not panel['quarterly']
 assert panel['sourceEvidence']['inlineFilingSupplement']['core_rounded_duplicates']==proof


def test_complete_original_incompatible_narrative_stays_held():
 raw=originals()[URL];assert raw.count(b'>27.6<')==1
 damaged=raw.replace(b'>27.6<',b'>28.6<')
 with pytest.raises(inline.InlineFactsError,match='conflicting_reported_facts'):
  inline.parse(damaged,cik=CIK,accession='0001193125-26-342307',filed='2026-08-10',report_date='2026-06-30',form='10-Q',url=URL)


def test_recovered_original_flows_through_score_watchlist_rebaseline_crossing_and_rollback(monkeypatch):
 import test_sec_tagged_foreign_consumers as consumer
 value=core.project_fundamentals(facts(),company(),symbol='DMRA',cik=CIK,observed_at=NOW,filing_supplement=acquire())
 metrics={k:v for k,v in value.items() if type(v) in (int,float)}
 monkeypatch.setattr(consumer,'originals',lambda symbol:(facts(),company(),CIK,originals()))
 monkeypatch.setattr(consumer,'EXPECTED',{'DMRA':{'metrics':metrics}})
 monkeypatch.setattr(consumer,'NOW',NOW)
 consumer.test_original_source_score_and_alert_recovery_repeat_crossing_rollback('DMRA','roe',monkeypatch)
