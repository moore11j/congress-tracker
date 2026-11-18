"""Bounded offline proof of reported-period preservation and scope holds."""
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

os.environ['DATABASE_URL'] = 'sqlite://'
os.environ['STOCK_PRICE_PROVIDER'] = 'fmp'
os.environ['SEC_FINANCIAL_DISCOVERY_READ_ENABLED'] = '0'


def denied(*args, **kwargs):
    raise AssertionError('External transport forbidden')


socket.socket.connect = denied
try:
    import resource
    import signal
    resource.setrlimit(resource.RLIMIT_CPU, (8, 10))
    signal.alarm(20)
except ImportError:
    pass

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from app.services import sec_fundamentals as sec, sec_reporting_scope as scope
from app.services import sec_financial_statements as statements

started = time.process_time()
fixture = ROOT / 'tests/fixtures/sec_reporting_scope'
manifest = json.loads((fixture / 'sources.json').read_bytes())
raw = {}
for name, meta in manifest.items():
    data = gzip.decompress((fixture / meta['file']).read_bytes())
    assert len(data) == meta['bytes'] and hashlib.sha256(data).hexdigest() == meta['sha256']
    raw[name] = data
for name, (url, digest) in scope.SOURCES.items():
    assert manifest[name]['url'] == url and manifest[name]['sha256'] == digest
urls = {meta['url']: raw[name] for name, meta in manifest.items()}
now = datetime(2026, 11, 17, 21, tzinfo=timezone.utc)
calls = []


class Clock(datetime):
    @classmethod
    def now(cls, tz=None):
        return now


class Client:
    def get(self, url):
        calls.append(url)
        return urls[url]


with patch.object(sec, 'datetime', Clock), patch.object(sec, 'company_directory',
        lambda _: {'NFE': {'cik': scope.CIK}}), patch('app.clients.direct_sources.DirectSourceClient', Client):
    value = sec.fetch_fundamentals('NFE')
    first = list(calls)
    calls.clear()
    assert sec.fetch_fundamentals('NFE') == value and calls == first
assert first == [manifest[k]['url'] for k in ('company', 'facts', 'current')]
assert value['free_cash_flow'] == -589920000
assert value['current_ratio'] == 891482 / 989560
assert not {'fcf_margin', 'revenue_growth', 'operating_margin', 'operating_margin_expansion',
    'net_margin', 'roe'} & value.keys()
evidence = json.loads(value['source_evidence_json'])
assert evidence['current']['revenue'] is None
assert evidence['reporting_scope'] == scope.policy(scope.CIK, now.date())
args = dict(symbol='NFE', cik=scope.CIK, observed_at=now)
panel = statements.project(raw['facts'], raw['company'], **args)
assert statements.project(raw['facts'], raw['company'], **args) == panel
assert not any(r['date'] == '2025-12-31' for r in panel['quarterly'])
assert any(r['date'] == '2025-12-31' and r['revenue'] == 1504037000 for r in panel['annual'])
assert any(r['date'] == '2026-09-30' and r['revenue'] == 335818000 for r in panel['quarterly'])
print('RESULT=' + json.dumps(dict(complete_originals=len(manifest),
    original_bytes=sum(m['bytes'] for m in manifest.values()), current_ratio=value['current_ratio'],
    free_cash_flow=value['free_cash_flow'], mixed_scope_ratios_held=4,
    reported_periods_preserved=True, mixed_scope_derived_quarter_held=True,
    exact_repeat=True, original_request_repeat=True, cpu_seconds=time.process_time() - started,
    production_writes=0, provider_requests=0, emails=0)))
