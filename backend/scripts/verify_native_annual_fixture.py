"""Bounded complete-original IFRS annual-anchor acquisition and reader proof."""
from pathlib import Path
from datetime import datetime,timezone
from unittest.mock import patch
from types import SimpleNamespace
import gzip,hashlib,json,os,socket,sys,time

os.environ['DATABASE_URL']='sqlite://'
os.environ['STOCK_PRICE_PROVIDER']='fmp'
os.environ['SEC_FINANCIAL_DISCOVERY_READ_ENABLED']='0'
def denied(*a,**k):raise AssertionError('External transport forbidden')
socket.socket.connect=denied
try:
    import resource,signal
    resource.setrlimit(resource.RLIMIT_CPU,(8,10))
    signal.alarm(20)
except ImportError:
    pass
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from app.services import sec_fundamentals as sec,sec_native_annual_history as annual
from app.services.fundamentals_cache import fundamentals_summary_from_cache_row
from app.services.confirmation_score import _fundamentals_context_source
started=time.process_time();fixture=ROOT/'tests/fixtures/sec_native_annual_history'
manifest=json.loads((fixture/'sources.json').read_bytes());raw={}
for name,meta in manifest.items():
    raw[name]=gzip.decompress((fixture/meta['file']).read_bytes())
    assert len(raw[name])==meta['bytes'] and hashlib.sha256(raw[name]).hexdigest()==meta['sha256']
urls={meta['url']:raw[name] for name,meta in manifest.items()};calls=[]
now=datetime(2026,11,16,4,30,tzinfo=timezone.utc)
class Clock(datetime):
    @classmethod
    def now(cls,tz=None):return now
class Client:
    issuer_document_urls=frozenset()
    def get(self,url):calls.append(url);return urls[url]
with patch.object(sec,'datetime',Clock),patch.object(sec,'company_directory',lambda _: {'ADSE':{'cik':'0001879248'}}),patch('app.clients.direct_sources.DirectSourceClient',Client):
    value=sec.fetch_fundamentals('ADSE');first=list(calls);calls.clear()
    assert sec.fetch_fundamentals('ADSE')==value and calls==first
assert first==[manifest[k]['url'] for k in ('company','facts','cover','current','annual')]
proof=json.loads(value['source_evidence_json']);assert annual.cached_valid(proof)
assert proof['current']['revenue']['value']==24297000
assert proof['prior']['revenue']['value']==45364000
expected=dict(revenue_growth=(24297000/45364000-1)*100,gross_margin=-16466000/24297000*100,
    operating_margin=-52931000/24297000*100,net_margin=-82449000/24297000*100,
    operating_margin_expansion=(-52931000/24297000+33665000/45364000)*100,fcf_margin=-28493000/24297000*100)
assert all(abs(value[k]-v)<1e-10 for k,v in expected.items())
assert value['current_ratio']==0.9362844043874885
assert not {'free_cash_flow','roe','net_debt_to_ebitda','ev_to_ebitda'} & value.keys()
assert proof['native_cash_flow']['currency']=='EUR' and proof['native_cash_flow']['current']==-28493000
summary=fundamentals_summary_from_cache_row(SimpleNamespace(**value),now=now)
assert summary['status']=='unavailable'
assert _fundamentals_context_source(summary)==_fundamentals_context_source(None)
print('RESULT='+json.dumps(dict(complete_originals=len(manifest),original_bytes=sum(m['bytes'] for m in manifest.values()),
    current_revenue=24297000,prior_revenue=45364000,added_metrics=expected,current_ratio=value['current_ratio'],
    currency='EUR',unsupported_metrics_held=True,classification='unavailable',confirmation_source_unchanged=True,
    annual_source_sha256=proof['inline_filing_supplement']['native_annual_history']['source_sha256'],
    exact_repeat=True,original_request_repeat=True,cpu_seconds=time.process_time()-started,
    production_writes=0,provider_requests=0,emails=0)))
