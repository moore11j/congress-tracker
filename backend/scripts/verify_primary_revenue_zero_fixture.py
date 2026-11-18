"""Bounded complete-original proof of a comparative primary-statement zero."""
from pathlib import Path
from datetime import datetime, timezone
from unittest.mock import patch
import gzip
import hashlib
import json
import os
import socket
import sys
import time

os.environ['DATABASE_URL']='sqlite://'
os.environ['STOCK_PRICE_PROVIDER']='fmp'
os.environ['SEC_FINANCIAL_DISCOVERY_READ_ENABLED']='0'
def denied(*args,**kwargs):raise AssertionError('External transport forbidden')
socket.socket.connect=denied
try:
    import resource
    import signal
    resource.setrlimit(resource.RLIMIT_CPU,(8,10))
    signal.alarm(20)
except ImportError:
    pass
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from app.services import sec_fundamentals as sec, sec_primary_revenue_zero as zero
from app.services import sec_inline_financials as inline

started=time.process_time()
fixture=ROOT/'tests/fixtures/sec_primary_revenue_zero'
manifest=json.loads((fixture/'sources.json').read_bytes())
raw={}
for name,meta in manifest.items():
    raw[name]=gzip.decompress((fixture/meta['file']).read_bytes())
    assert len(raw[name])==meta['bytes'] and hashlib.sha256(raw[name]).hexdigest()==meta['sha256']
urls={meta['url']:raw[name] for name,meta in manifest.items()}
now=datetime(2026,11,16,4,30,tzinfo=timezone.utc)
calls=[]
class Clock(datetime):
    @classmethod
    def now(cls,tz=None):return now
class Client:
    issuer_document_urls=frozenset()
    def get(self,url):
        calls.append(url)
        return urls[url]
with patch.object(sec,'datetime',Clock), patch.object(sec,'company_directory',lambda _: {'ADAG':{'cik':'0001818838'}}), patch('app.clients.direct_sources.DirectSourceClient',Client):
    value=sec.fetch_fundamentals('ADAG')
    first=list(calls);calls.clear()
    assert sec.fetch_fundamentals('ADAG')==value and calls==first
assert first==[manifest[k]['url'] for k in ('company','facts','cover','current')]
evidence=json.loads(value['source_evidence_json'])
assert zero.cached_valid(evidence)
assert evidence['current']['revenue']['value']==9290815
assert value['net_margin']==-20511005/9290815*100
assert value['operating_margin']==-22336577/9290815*100
assert value['current_ratio']==132408623/26361197
assert value['roe']==-20511005/((37911789+89453418)/2)*100
assert not {'free_cash_flow','fcf_margin','revenue_growth','net_debt_to_ebitda'} & value.keys()
annual=inline.parse(raw['annual'],cik='0001818838',accession='0001104659-26-038612',
    filed='2026-04-01',report_date='2025-12-31',form='20-F',url=manifest['annual']['url'])
annual_points=annual['facts'][zero.REVENUE]['units']['USD']
assert any(p.get('start')=='2025-01-01' and p['end']=='2025-12-31' and p['val']==7670902 for p in annual_points)
assert evidence['inline_filing_supplement']['primary_comparative_revenue_zeros'][0]['value']==0
print('RESULT='+json.dumps(dict(complete_originals=len(manifest),original_bytes=sum(m['bytes'] for m in manifest.values()),
    current_revenue=9290815,net_margin=value['net_margin'],operating_margin=value['operating_margin'],
    current_ratio=value['current_ratio'],roe=value['roe'],added_metrics=2,unsupported_metrics_held=True,
    exact_repeat=True,original_request_repeat=True,cpu_seconds=time.process_time()-started,
    production_writes=0,provider_requests=0,emails=0)))
