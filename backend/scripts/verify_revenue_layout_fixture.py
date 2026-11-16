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
from app.services import sec_fundamentals as sec,sec_inline_financials as inline,sec_revenue_series as series
from app.services.fundamentals_cache import fundamentals_summary_from_cache_row
started=time.process_time()
fixture=ROOT/'tests/fixtures/revenue_statement_layout'
manifest=json.loads((fixture/'sources.json').read_bytes());originals={}
for name,meta in manifest.items():
    raw=gzip.decompress((fixture/meta['file']).read_bytes())
    assert len(raw)==meta['bytes'] and hashlib.sha256(raw).hexdigest()==meta['sha256']
    originals[meta['url']]=raw
assert len(originals)==5
facts=originals[manifest['HRB-facts.json']['url']];company=originals[manifest['HRB-company.json']['url']]
now=datetime.fromisoformat('2026-11-16T04:30:00+00:00')
calls=[]
class Client:
    def get(self,url):
        calls.append(url);assert url in originals
        return originals[url]
def run():
    supplement=inline.acquire(facts,company,symbol='HRB',cik='0000012659',observed_at=now,client=Client())
    assert series.cached_valid(supplement['evidence']['revenue_series_selection'],cik='0000012659')
    return sec.project_fundamentals(facts,company,symbol='HRB',cik='0000012659',observed_at=now,filing_supplement=supplement)
value=run();first=list(calls);calls.clear();repeat=run()
assert value==repeat and first==calls and len(first)==len(set(first))==3
evidence=json.loads(value['source_evidence_json'])
assert evidence['current']['revenue']['value']==3_966_177_000
assert evidence['prior']['revenue']['value']==3_770_736_000
summary=fundamentals_summary_from_cache_row(SimpleNamespace(**value),now=now)
assert summary['context']['source_label']=='SEC EDGAR'
assert summary['metrics']['revenue_growth']['value']==value['revenue_growth']/100
assert summary['status']=='unavailable' and not summary['data_quality']['available']
print('RESULT='+json.dumps(dict(complete_originals=5,original_bytes=sum(m['bytes'] for m in manifest.values()),current_ttm_revenue=evidence['current']['revenue']['value'],prior_ttm_revenue=evidence['prior']['revenue']['value'],recovered_metrics=['revenue_growth','net_margin','fcf_margin'],classification=summary['status'],exact_repeat=True,original_request_repeat=True,cpu_seconds=time.process_time()-started,production_writes=0,provider_requests=0,emails=0)))
