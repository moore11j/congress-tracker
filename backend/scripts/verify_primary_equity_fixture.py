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
from app.services import sec_fundamentals as sec,sec_primary_equity as equity
from app.services.fundamentals_cache import fundamentals_summary_from_cache_row
from types import SimpleNamespace
started=time.process_time();fixture=root/'tests/fixtures/sec_primary_equity'
manifest=json.loads((fixture/'sources.json').read_bytes());raw={}
for key,m in manifest.items():
    raw[key]=gzip.decompress((fixture/m['file']).read_bytes())
    assert len(raw[key])==m['bytes'] and hashlib.sha256(raw[key]).hexdigest()==m['sha256']
urls={m['url']:raw[k] for k,m in manifest.items()};calls=[]
company=json.loads(raw['company']);cik=str(company['cik']).zfill(10)
now=datetime(2026,11,18,19,45,tzinfo=timezone.utc)
class Clock(datetime):
    @classmethod
    def now(cls,tz=None):return now
class Client:
    issuer_document_urls=frozenset()
    def get(self,url):calls.append(url);return urls[url]
before=sec.project_fundamentals(raw['facts'],raw['company'],symbol='LGIH',cik=cik,observed_at=now)
with patch.object(sec,'datetime',Clock),patch.object(sec,'company_directory',lambda _: {'LGIH':{'cik':cik}}),patch('app.clients.direct_sources.DirectSourceClient',Client):
    after=sec.fetch_fundamentals('LGIH');first=list(calls);calls.clear()
    assert sec.fetch_fundamentals('LGIH')==after and calls==first
assert len(first)==5 and set(first)=={m['url'] for m in manifest.values()}
assert all(after[k]==v for k,v in before.items() if k!='source_evidence_json')
assert abs(after['roe']-3.3502051983584127)<1e-9
assert set(after)-set(before)=={'roe'}
assert equity.cached_valid(json.loads(after['source_evidence_json']))
before_status=fundamentals_summary_from_cache_row(SimpleNamespace(**before),now=now)['status']
after_status=fundamentals_summary_from_cache_row(SimpleNamespace(**after),now=now)['status']
print('RESULT='+json.dumps(dict(complete_fixtures=len(manifest),original_bytes=sum(m['bytes'] for m in manifest.values()),
    added_metrics=sorted(set(after)-set(before)),non_null_values_preserved=True,exact_repeat=True,requests_identical_on_repeat=True,
    classification_before=before_status,classification_after=after_status,cpu_seconds=time.process_time()-started,
    production_writes=0,provider_requests=0,emails=0)))
