from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
import gzip
import hashlib
import json

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.models import InsightsSnapshot
from app.clients.direct_sources import DirectSourceError
from app.services import sec_directory, sec_financial_statements as statements
from app.services import sec_inline_financials as inline, sec_registration_history as registration
from app.services.sec_financial_continuity import ContinuityError

ROOT = Path(__file__).parent/'fixtures/sec_registration_rollover_nov6'
NOW = datetime(2026, 11, 6, 23, 40, tzinfo=timezone.utc)


@pytest.fixture
def context(monkeypatch):
    manifest = json.loads((ROOT/'manifest.json').read_bytes())
    sources = {}
    for url, entry in manifest['sources'].items():
        raw = gzip.decompress((ROOT/entry['file']).read_bytes())
        assert len(raw) == entry['bytes'] and hashlib.sha256(raw).hexdigest() == entry['sha256']
        sources[url] = raw
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine, tables=[InsightsSnapshot.__table__])
    factory = sessionmaker(bind=engine)
    monkeypatch.setattr(statements, 'SessionLocal', factory)
    monkeypatch.setattr(sec_directory, 'directory', lambda: {'SPCX': {'cik': registration.CIK}})
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return NOW.astimezone(tz) if tz else NOW.replace(tzinfo=None)
    monkeypatch.setattr(statements, 'datetime', Clock)
    monkeypatch.setenv('SEC_FINANCIAL_DISCOVERY_READ_ENABLED', '0')
    calls = []
    def fetch(self, url):
        calls.append(url)
        assert url in sources, 'Unexpected source request'
        return sources[url]
    monkeypatch.setattr(statements.DirectSourceClient, 'get', fetch)
    yield SimpleNamespace(factory=factory, calls=calls, sources=sources, manifest=manifest)
    engine.dispose()


def test_complete_current_sources_keep_hold_and_cache_truthful_unavailable(context, monkeypatch):
    company_url = 'https://data.sec.gov/submissions/CIK0001181412.json'
    facts_url = 'https://data.sec.gov/api/xbrl/companyfacts/CIK0001181412.json'
    company = json.loads(context.sources[company_url])
    with pytest.raises(registration.RegistrationHistoryError, match='registration_newer_or_amended_report_requires_review'):
        registration.validate_company(company, symbol='SPCX', cik=registration.CIK, observed_at=NOW)
    result = statements.prepared('SPCX')
    assert result['status'] == 'unavailable' and result['reason'] == 'unverified_financial_history'
    assert result['annual'] == result['quarterly'] == []
    proof = result['sourceEvidence']
    assert proof['factsSha256'] == hashlib.sha256(context.sources[facts_url]).hexdigest()
    assert proof['companySha256'] == hashlib.sha256(context.sources[company_url]).hexdigest()
    assert proof['coverageHold'] == dict(policy=statements.HISTORY_HOLD_POLICY,
        code='registration_newer_or_amended_report_requires_review', errorType='RegistrationHistoryError')
    before = list(context.calls)
    assert before == [company_url, facts_url]
    assert statements.prepared('SPCX') == result
    assert context.calls == before
    from app.services import ticker_financials, data_enrichment_queue
    from app.request_priority import set_request_context, reset_request_context
    monkeypatch.setenv('FINANCIAL_STATEMENTS_PROVIDER', 'sec_edgar')
    monkeypatch.setattr(data_enrichment_queue, 'enqueue_data_enrichment_job', lambda **kw: pytest.fail('Cached source hold was requeued'))
    token = set_request_context({'path': '/api/tickers/SPCX/financials'})
    try:
        public = ticker_financials.get_ticker_financials('SPCX')
        assert public['status'] == 'unavailable' and public['reason'] == result['reason']
        assert public['annual'] == public['quarterly'] == []
    finally:
        reset_request_context(token)
    assert context.calls == before
    with context.factory() as db:
        row = db.get(InsightsSnapshot, 'sec-financials:SPCX:v1')
        assert statements.cached_row_payload(row, 'SPCX', now=NOW+timedelta(hours=23)) == result
        assert statements.cached_row_payload(row, 'SPCX', now=NOW+timedelta(hours=24)) is None
        assert statements.cached_row_payload(row, 'OTHER', now=NOW) is None


def test_current_quarter_original_remains_parseable_without_waiving_history_hold(context):
    filing = context.manifest['diagnosis']['latest_quarter']
    parsed = inline.parse(context.sources[filing['url']], cik=registration.CIK,
        accession=filing['accession'], filed=filing['filed'], report_date=filing['end'],
        form='10-Q', url=filing['url'])
    assert parsed['evidence']['reported_facts'] == 52
    assert parsed['evidence']['report_date'] == '2026-09-30'
    assert statements.prepared('SPCX')['status'] == 'unavailable'


@pytest.mark.parametrize('error', [registration.RegistrationHistoryError('registration_projection_changed'),
                                 ContinuityError('unreviewed_accounting_continuity')])
def test_known_history_errors_are_explicit_results(context, monkeypatch, error):
    def fail(*a, **kw): raise error
    monkeypatch.setattr(inline, 'acquire', fail)
    payload = statements.prepared('SPCX')
    assert payload['sourceEvidence']['coverageHold']['code'] == str(error)
    assert statements.prepared('SPCX') == payload and len(context.calls) == 2


@pytest.mark.parametrize('error', [ValueError('unexpected defect'), TypeError('unexpected defect'),
                                 DirectSourceError('Source HTTP 429: https://data.sec.gov/test')])
def test_unexpected_or_transport_failure_is_not_cached(context, monkeypatch, error):
    def fail(*a, **kw): raise error
    monkeypatch.setattr(inline, 'acquire', fail)
    with pytest.raises(type(error), match='unexpected defect|Source HTTP 429'):
        statements.prepared('SPCX')
    with context.factory() as db:
        assert db.scalar(select(InsightsSnapshot)) is None


@pytest.mark.parametrize('change', ['policy', 'code', 'status'])
def test_stale_or_malformed_history_hold_is_not_reused(context, change):
    payload = statements.prepared('SPCX')
    if change == 'policy': payload['sourceEvidence']['coverageHold']['policy'] = 'old'
    elif change == 'code': payload['sourceEvidence']['coverageHold']['code'] = ''
    else: payload['status'] = 'ok'
    with context.factory() as db:
        row = db.get(InsightsSnapshot, 'sec-financials:SPCX:v1')
        row.payload_json = json.dumps(payload)
        assert statements.cached_row_payload(row, 'SPCX', now=NOW) is None


def test_non_code_exception_detail_is_not_copied_to_public_payload(context, monkeypatch):
    def fail(*a, **kw): raise ContinuityError('unreviewed internal detail with spaces')
    monkeypatch.setattr(inline, 'acquire', fail)
    result = statements.prepared('SPCX')
    assert result['sourceEvidence']['coverageHold']['code'] == 'ContinuityError'
    assert 'internal detail' not in json.dumps(result)
