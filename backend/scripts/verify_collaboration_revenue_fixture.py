"""Complete originals and revenue-period proof within unchanged offline limits."""
from pathlib import Path
from datetime import datetime
from types import SimpleNamespace
import gzip,hashlib,json,os,socket,sys,time

os.environ['DATABASE_URL']='sqlite://'
def denied(*a,**k):raise AssertionError('External transport forbidden')
socket.socket.connect=denied
try:
    import resource,signal
    resource.setrlimit(resource.RLIMIT_CPU,(10,12));signal.alarm(22)
except ImportError:
    pass
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from app.services import sec_fundamentals as sec,sec_inline_financials as inline,sec_collaboration_revenue as series
from app.services.fundamentals_cache import fundamentals_summary_from_cache_row
started=time.process_time()
fixture=ROOT/'tests/fixtures/sec_collaboration_revenue'
manifest=json.loads((fixture/'sources.json').read_bytes());originals={}
for name,meta in manifest.items():
    raw=gzip.decompress((fixture/meta['file']).read_bytes())
    assert len(raw)==meta['bytes'] and hashlib.sha256(raw).hexdigest()==meta['sha256']
    originals[meta['url']]=raw
assert len(originals)==5
facts=originals[manifest['HOWL-facts.json']['url']];company=originals[manifest['HOWL-company.json']['url']]
now=datetime.fromisoformat('2026-11-16T04:30:00+00:00')
calls=[]
class Client:
    def get(self,url):
        calls.append(url);assert url in originals
        return originals[url]
def run():
    supplement=inline.acquire(facts,company,symbol='HOWL',cik='0001785530',observed_at=now,client=Client())
    assert series.cached_valid(supplement['evidence']['collaboration_revenue_selection'],cik='0001785530')
    return sec.project_fundamentals(facts,company,symbol='HOWL',cik='0001785530',observed_at=now,filing_supplement=supplement)
value=run();first=list(calls);calls.clear();repeat=run()
assert value==repeat and first==calls and len(first)==len(set(first))==3
evidence=json.loads(value['source_evidence_json'])
assert evidence['current']['revenue']['value']==21_000_000
assert evidence['prior']['revenue']['value']==0
summary=fundamentals_summary_from_cache_row(SimpleNamespace(**value),now=now)
assert summary['context']['source_label']=='SEC EDGAR'
assert value.get('revenue_growth') is None and value.get('operating_margin_expansion') is None
assert value['operating_margin'] == -121.83333333333333
assert value['net_margin'] == -133.9047619047619
assert value.get('fcf_margin') is None and value.get('free_cash_flow') is None
assert evidence['current']['capex'] is None
assert summary['status']=='unavailable' and not summary['data_quality']['available']
print('RESULT='+json.dumps(dict(complete_originals=5,original_bytes=sum(m['bytes'] for m in manifest.values()),current_ttm_revenue=evidence['current']['revenue']['value'],prior_ttm_revenue=evidence['prior']['revenue']['value'],recovered_metrics=['operating_margin','net_margin'],fcf_margin_unavailable_missing_capex=True,classification=summary['status'],exact_repeat=True,original_request_repeat=True,cpu_seconds=time.process_time()-started,production_writes=0,provider_requests=0,emails=0)))
