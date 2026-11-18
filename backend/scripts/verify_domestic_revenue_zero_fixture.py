"""Bounded full-reader replay; no external transport, database writes or sends."""
from pathlib import Path
from datetime import datetime,timezone
from unittest.mock import patch
import gzip,hashlib,json,os,socket,sys,time
os.environ['DATABASE_URL']='sqlite://'
os.environ['SEC_FINANCIAL_DISCOVERY_READ_ENABLED']='0'
def denied(*a,**k):raise AssertionError('External transport forbidden')
socket.socket.connect=denied
try:
    import resource,signal
    resource.setrlimit(resource.RLIMIT_CPU,(8,10));signal.alarm(20)
except ImportError:
    pass
root=Path(__file__).resolve().parents[1];sys.path.insert(0,str(root))
from app.services import sec_fundamentals as sec,sec_domestic_revenue_zeros as zero
from app.services.fundamentals_cache import fundamentals_summary_from_cache_row
from types import SimpleNamespace
started=time.process_time();fixture=root/'tests/fixtures/sec_domestic_revenue_zeros'
manifest=json.loads((fixture/'sources.json').read_bytes());raw={}
for key,m in manifest.items():
    raw[key]=gzip.decompress((fixture/m['file']).read_bytes())
    assert len(raw[key])==m['bytes'] and hashlib.sha256(raw[key]).hexdigest()==m['sha256']
urls={m['url']:raw[k] for k,m in manifest.items()};calls=[]
company=json.loads(raw['MNOV:company']);cik=str(company['cik']).zfill(10)
now=datetime(2026,11,18,19,45,tzinfo=timezone.utc)
class Clock(datetime):
    @classmethod
    def now(cls,tz=None):return now
class Client:
    issuer_document_urls=frozenset()
    def get(self,url):calls.append(url);return urls[url]
before=sec.project_fundamentals(raw['MNOV:facts'],raw['MNOV:company'],symbol='MNOV',cik=cik,observed_at=now)
with patch.object(sec,'datetime',Clock),patch.object(sec,'company_directory',lambda _: {'MNOV':{'cik':cik}}),patch('app.clients.direct_sources.DirectSourceClient',Client):
    after=sec.fetch_fundamentals('MNOV');first=list(calls);calls.clear()
    assert sec.fetch_fundamentals('MNOV')==after and calls==first
assert first==[manifest['MNOV:'+k]['url'] for k in ('company','facts','annual','prior')]
assert all(after[k]==v for k,v in before.items() if k!='source_evidence_json')
assert abs(after['operating_margin_expansion']-3953.321074957527)<1e-9
assert zero.cached_valid(json.loads(after['source_evidence_json']))
assert fundamentals_summary_from_cache_row(SimpleNamespace(**before),now=now)['status']=='unavailable'
assert fundamentals_summary_from_cache_row(SimpleNamespace(**after),now=now)['status']=='mixed'
print('RESULT='+json.dumps(dict(complete_fixtures=len(manifest),original_bytes=sum(m['bytes'] for m in manifest.values()),
    added_metrics=sorted(set(after)-set(before)),non_null_values_preserved=True,exact_repeat=True,requests_identical_on_repeat=True,
    classification_before='unavailable',classification_after='mixed',cpu_seconds=time.process_time()-started,
    production_writes=0,provider_requests=0,emails=0)))
