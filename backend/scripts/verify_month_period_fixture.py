"""Bounded complete-source period proof without provider, database or delivery access."""
import os,socket,resource,signal,time,gzip,hashlib,json
from pathlib import Path
from datetime import date,datetime,timezone
resource.setrlimit(resource.RLIMIT_CPU,(10,12));signal.alarm(22)
os.environ['DATABASE_URL']='sqlite://'
def denied(*a,**k):raise AssertionError('External transport forbidden')
socket.socket.connect=denied
ROOT=Path(__file__).resolve().parents[1]
start=time.process_time()
from app.services import sec_tabular_financials as tables,sec_tabular_reporting as reporting,sec_financial_discovery as discovery,sec_financial_discovery_reader as reader
from app.services.sec_document_sets import candidates
sources=ROOT/'tests/fixtures/sec_us_conformity'
manifest=json.loads((sources/'sources.json').read_bytes())
def load(root,item):
 raw=gzip.decompress((root/item['file']).read_bytes())
 assert len(raw)==item['bytes'] and hashlib.sha256(raw).hexdigest()==item['sha256']
 return raw
company=load(sources,manifest['other_companies']['SB'])
statement=next(r for r in manifest['sources'] if r['candidate']['accession']=='0001317861-26-000056')
raw=load(sources,statement)
facts_root=ROOT/'tests/fixtures/sec_month_period_headers'
facts=load(facts_root,json.loads((facts_root/'sources.json').read_bytes())['facts'])
common=dict(symbol='SB',cik='0001434754',available_by=date(2026,11,15),observed_at=datetime(2026,11,15,23,tzinfo=timezone.utc))
checkpoint=discovery.inspect(company,**common)['state']
candidate=next(c for c in candidates(company,symbol='SB',cik=common['cik'],available_by=common['available_by']) if c['cover_url']==statement['url'])
identity={k:candidate[k] for k in discovery.IDENTITY_FIELDS}
checkpoint['receipts'][discovery._fingerprint(identity)]=discovery._observe(raw,identity,common['observed_at'])
class Client:
 def __init__(self):self.calls=[]
 def get(self,url):
  self.calls.append(url);assert url==statement['url'];return raw
def inspect():
 client=Client()
 result=reader.inspect_candidate(company,facts,**common,checkpoint=checkpoint,accession=candidate['accession'],client=client)
 assert client.calls==[statement['url']]
 return result
result=inspect();assert result==inspect()
assert result['receipt']['status']=='qualified' and result['receipt']['current_period']=='2026-09-30'
assert not result['receipt']['financial_publication_eligible']
normalized=reporting.validate(result['supplement'],company_raw=company,symbol='SB',cik=common['cik'],available_by=common['available_by'],facts_raw=facts)
records=normalized['records']
assert records['operating_income'][date(2026,7,1),date(2026,9,30)]['value']==45868000
assert records['operating_cash_flow'][date(2026,1,1),date(2026,9,30)]['value']==136196000
assert len(records['revenue'])==4 and all(v is None for v in records['revenue'].values())
assert not records['net_income'] and not records['equity']
assert normalized['evidence']['native_observation_count']==12
original=tables.month_period_wording
try:
 tables.month_period_wording=lambda text:text
 baseline=inspect()
 assert baseline['receipt']['status']=='held'
 assert baseline['receipt']['holds'][0]['reason']=='table_current_statement_alignment'
finally:tables.month_period_wording=original
print('RESULT='+json.dumps(dict(complete_originals=3,qualification='html_table',current_period='2026-09-30',native_observations=12,exact_repeat=True,prior_alignment_hold_verified=True,revenue_conflicts_retained=4,parent_income_unavailable=True,production_writes=0,provider_requests=0,emails=0,cpu_seconds=time.process_time()-start)))
