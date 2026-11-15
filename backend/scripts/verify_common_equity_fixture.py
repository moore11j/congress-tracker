"""Finite offline source-to-consumer acceptance; no database or provider access."""
import os
import signal
import socket
import time

os.environ['DATABASE_URL']='sqlite://'
os.environ['SEC_FINANCIAL_DISCOVERY_READ_ENABLED']='0'

def denied(*args,**kwargs):
    raise AssertionError('External transport forbidden')

socket.socket.connect=denied
if os.name!='nt':
    import resource
    resource.setrlimit(resource.RLIMIT_CPU,(10,12))
    signal.alarm(22)

from pathlib import Path
from datetime import datetime,timezone
from copy import deepcopy
from types import SimpleNamespace
import gzip,hashlib,json,sys

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from app.services import sec_inline_financials as inline,sec_fundamentals as core,sec_financial_statements as panels,sec_common_equity as equity
from app.services.fundamentals_cache import fundamentals_summary_from_cache_row

folder=ROOT/'tests/fixtures/sec_common_equity'
manifest=json.loads((folder/'sources.json').read_bytes());sources={}
for meta in manifest.values():
    raw=gzip.decompress((folder/meta['file']).read_bytes())
    assert len(raw)==meta['bytes'] and hashlib.sha256(raw).hexdigest()==meta['sha256']
    sources[meta['url']]=raw
company=sources['https://data.sec.gov/submissions/CIK0000922224.json']
facts=sources['https://data.sec.gov/api/xbrl/companyfacts/CIK0000922224.json']
class Client:
    def __init__(self):self.calls=[]
    def get(self,url):self.calls.append(url);return sources[url]
now=datetime(2026,11,9,7,tzinfo=timezone.utc);client=Client()
assert equity._statement_rows.cache_info().currsize==0
item=inline.acquire(facts,company,symbol='PPLC',cik='0000922224',observed_at=now,client=client)
assert len(client.calls)==len(set(client.calls))==2
args=dict(symbol='PPLC',cik='0000922224',observed_at=now,filing_supplement=item)
value=core.project_fundamentals(facts,company,**args)
assert value['roe']==1315000000/((15209000000+14413000000)/2)*100
assert value.get('net_debt_to_ebitda') is None and value.get('ev_to_ebitda') is None
summary=fundamentals_summary_from_cache_row(SimpleNamespace(**value),now=now)
assert summary['metrics']['return_on_equity']['provenance']['version']==equity.VERSION
proof=item['evidence']['common_equity_reconciliation']
assert len(proof['rows'])==2 and equity.cached_valid(proof,cik='0000922224')
changed=deepcopy(proof);changed['rows'][0]['value']+=1
assert not equity.cached_valid(changed,cik='0000922224')
assert core.project_fundamentals(facts,company,**args)==value
panel=panels.project(facts,company,**args)
row=SimpleNamespace(source='sec_edgar',fetched_at=now,payload_json=json.dumps(panel))
assert panels.cached_row_payload(row,'PPLC',now=now)==panel
assert panels.cached_row_payload(row,'PPLC',now=now)==panel
print('RESULT='+json.dumps(dict(retained_public_sources=len(sources),original_requests=len(client.calls),qualified_periods=len(proof['rows']),roe=value['roe'],calculated_basis_displayed=True,public_cached_reader_repeat=True,changed_proof_rejected=True,production_writes=0,provider_requests=0,emails=0,cpu_seconds=time.process_time())))
