"""Offline complete-source currency-conflict and identity-only reader proof."""
from pathlib import Path
from datetime import datetime, timezone
from unittest.mock import patch
import gzip, hashlib, json, os, socket, sys, time
os.environ['DATABASE_URL']='sqlite://'
def denied(*a,**k):raise AssertionError('External transport forbidden')
socket.socket.connect=denied
try:
    import resource,signal
    resource.setrlimit(resource.RLIMIT_CPU,(6,8));signal.alarm(18)
except ImportError:
    pass
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from app.services import sec_fundamentals as sec,sec_statement_currency as currency
from app.services.independent_fundamentals import IndependentMetricsEligible
started=time.process_time();fixture=ROOT/'tests/fixtures/sec_currency_conflicts'
manifest=json.loads((fixture/'sources.json').read_bytes());sources={}
for key,meta in manifest.items():
    raw=gzip.decompress((fixture/meta['file']).read_bytes())
    assert len(raw)==meta['bytes'] and hashlib.sha256(raw).hexdigest()==meta['sha256']
    url=meta.get('url') or ('https://data.sec.gov/submissions/CIK0001905511.json' if key=='company' else 'https://data.sec.gov/api/xbrl/companyfacts/CIK0001905511.json')
    sources[url]=raw
assert len(sources)==3
url=manifest['JCSE']['url'];proof=currency.declaration(sources[url],url=url)
assert proof['status']=='ambiguous' and {r['currency'] for r in proof['declarations']}=={'SGD','USD'}
assert not currency.cached_currency_valid({'inline_filing_supplement':{'native_reporting':{},'statement_currency_declaration':{'source_sha256':manifest['JCSE']['sha256']}}})
class Clock(datetime):
    @classmethod
    def now(cls,tz=None):return datetime(2026,11,16,4,30,tzinfo=timezone.utc)
calls=[]
class Client:
    def get(self,url):calls.append(url);return sources[url]
def fetch():
    try:sec.fetch_fundamentals('JCSE')
    except IndependentMetricsEligible as exc:
        assert str(exc)=='statement_currency_ambiguous'
        return exc.values
    raise AssertionError('Conflicting statement must be held')
with patch.object(sec,'datetime',Clock),patch.object(sec,'company_directory',lambda _: {'JCSE':{'cik':'0001905511'}}),patch('app.clients.direct_sources.DirectSourceClient',Client):
    value=fetch();first=list(calls);calls.clear();assert fetch()==value and calls==first
assert len(first)==len(set(first))==3
evidence=json.loads(value['source_evidence_json'])
assert evidence['financial_status']=='unavailable' and value['period_date'] is None
assert not any(k in value for k in ('roe','current_ratio','free_cash_flow','fcf_growth'))
print('RESULT='+json.dumps(dict(complete_originals=3,original_bytes=sum(m['bytes'] for m in manifest.values()),
    hold='statement_currency_ambiguous',identity_preserved=True,legacy_cache_rejected=True,
    exact_repeat=True,original_request_repeat=True,cpu_seconds=time.process_time()-started,
    production_writes=0,provider_requests=0,emails=0)))
