"""Complete-original financial source attribution under bounded offline limits."""
from pathlib import Path
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import patch
import gzip,hashlib,json,os,socket,sys,time

os.environ['DATABASE_URL']='sqlite://'
os.environ['STOCK_PRICE_PROVIDER']='fmp'
os.environ['SEC_FINANCIAL_DISCOVERY_READ_ENABLED']='0'
def denied(*a,**k):raise AssertionError('External transport forbidden')
socket.socket.connect=denied
try:
    import resource,signal
    resource.setrlimit(resource.RLIMIT_CPU,(10,12));signal.alarm(22)
except ImportError:
    pass
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from app.services import sec_fundamentals as sec,free_fundamentals as free,finnhub_metrics
from app.services.fundamentals_cache import fundamentals_summary_from_cache_row

started=time.process_time()
fixture=ROOT/'tests/fixtures/fallback_source_label'
manifest=json.loads((fixture/'sources.json').read_bytes());originals={}
for meta in manifest['sources']:
    raw=gzip.decompress((fixture/meta['file']).read_bytes())
    assert len(raw)==meta['bytes'] and hashlib.sha256(raw).hexdigest()==meta['sha256']
    originals[meta.get('url',meta.get('endpoint'))]=raw
assert len(originals)==3
now=datetime.fromisoformat(manifest['observed_at'])
class Clock(datetime):
    @classmethod
    def now(cls,tz=None):return now
calls=[]
class Client:
    def get(self,url):
        calls.append(url);assert url in originals
        return originals[url]
metrics=finnhub_metrics.normalize(json.loads(originals['stock/metric'])['payload'],symbol='BABA',observed_at=now)
with patch.object(sec,'datetime',Clock),patch.object(sec,'company_directory',lambda _:{'BABA':{'cik':manifest['cik']}}),patch('app.clients.direct_sources.DirectSourceClient',Client),patch.object(finnhub_metrics,'prepare',lambda _:metrics):
    values=free.fetch_fundamentals('BABA')
    assert values==free.fetch_fundamentals('BABA') and len(calls)==4
evidence=json.loads(values['source_evidence_json'])
assert evidence['financial_status']=='unavailable' and not evidence.get('independent_metrics')
assert evidence['current']==evidence['prior']==evidence['instant']==evidence['calculations']=={}
assert all(values[field]==value for field,value in metrics['values'].items())
summary=fundamentals_summary_from_cache_row(SimpleNamespace(**values),now=now)
assert summary['context']['source_label']=='Finnhub' and summary['data_quality']['available']
assert 'Company identity verified with SEC EDGAR' in summary['context']['source_note']
print('RESULT='+json.dumps(dict(complete_originals=3,original_bytes=sum(m['bytes'] for m in manifest['sources']),supported_metrics=len(metrics['values']),classification=summary['status'],source_label=summary['context']['source_label'],exact_repeat=True,cpu_seconds=time.process_time()-started,production_writes=0,provider_requests=0,emails=0)))
