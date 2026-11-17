"""Complete reverse-acquisition originals and consumers under offline limits."""
from pathlib import Path
from datetime import datetime
from types import SimpleNamespace
import gzip
import hashlib
import json
import os
import socket
import sys
import time

os.environ['DATABASE_URL'] = 'sqlite://'
def denied(*args, **kwargs):
    raise AssertionError('External transport forbidden')
socket.socket.connect = denied
try:
    import resource, signal
    resource.setrlimit(resource.RLIMIT_CPU, (10, 12))
    signal.alarm(22)
except ImportError:
    pass
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from app.services import sec_fundamentals as sec, sec_inline_financials as inline
from app.services import sec_reverse_acquisition as reverse, sec_common_income as common
from app.services import sec_financial_statements as statements
from app.services.fundamentals_cache import fundamentals_summary_from_cache_row

started = time.process_time()
fixture = ROOT/'tests/fixtures'/('sec_reverse_acquisition_delivery' if '--delivery' in sys.argv else 'sec_reverse_acquisition')
manifest = json.loads((fixture/'sources.json').read_bytes())
originals = {}
for name, source in manifest.items():
    raw = gzip.decompress((fixture/source['file']).read_bytes())
    assert len(raw) == source['bytes'] and hashlib.sha256(raw).hexdigest() == source['sha256']
    originals[source['url']] = raw
assert len(originals) == 5
facts = originals[manifest['VMRK-facts.json']['url']]
company = originals[manifest['VMRK-company.json']['url']]
now = datetime.fromisoformat('2026-11-17T04:00:00+00:00')
calls = []
class Client:
    def get(self, url):
        calls.append(url)
        assert url in originals
        return originals[url]
args = dict(symbol='VMRK', cik=reverse.CIK, observed_at=now)
source = inline.acquire(facts, company, client=Client(), **args)
value = sec.project_fundamentals(facts, company, filing_supplement=source, **args)
first = list(calls); calls.clear()
repeat_source = inline.acquire(facts, company, client=Client(), **args)
assert sec.project_fundamentals(facts, company, filing_supplement=repeat_source, **args) == value
assert first == calls and len(first) == len(set(first)) == 3
evidence = json.loads(value['source_evidence_json'])
assert reverse.cached_valid(evidence)
assert evidence['current']['revenue']['value'] == 3_511_386_000
assert evidence['current']['net_income']['value'] == -88_966_000
assert evidence['current']['operating_cash_flow']['value'] == 1_442_305_000
assert value['roe'] == -0.3722803186355641
assert value.get('free_cash_flow') is None and value.get('net_debt_to_ebitda') is None
income = common.resolve(facts, company, evidence, filing_supplement=source, **args)
assert income['point']['value'] == -89_321_000
panel = statements.project(facts, company, filing_supplement=source, **args)
assert panel['summary']['revenueTtm'] == 3_511_386_000
assert panel['summary']['netIncomeTtm'] == -88_966_000
summary = fundamentals_summary_from_cache_row(SimpleNamespace(**value), now=now)
assert summary['status'] == 'unavailable' and summary['context']['source_label'] == 'SEC EDGAR'
print('RESULT='+json.dumps(dict(complete_originals=5, original_bytes=sum(m['bytes'] for m in manifest.values()),
    current_ttm_revenue=3_511_386_000, current_parent_income=-88_966_000, common_income=-89_321_000,
    recovered_metrics=['revenue_growth', 'net_margin'], corrected_metrics=['roe'],
    unavailable=['capex', 'free_cash_flow', 'per_share_history', 'sector_leverage'],
    classification=summary['status'], exact_repeat=True, original_request_repeat=True,
    cpu_seconds=time.process_time()-started, production_writes=0, provider_requests=0, emails=0)))
