"""Whole public originals through discovery, partial ratios and alert consumers."""
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
import gzip, hashlib, json, socket
import pytest
from app.services import sec_fundamentals as sec, sec_tagged_foreign as tagged

ROOT = Path(__file__).parent/'fixtures/sec_qualified_foreign_partial'
NOW = datetime(2026, 10, 31, 22, 46, tzinfo=timezone.utc)
CIK = '0001818838'
EXPECTED = {'current_ratio': 132408623/26361197, 'roe': -20511005/((37911789+89453418)/2)*100}


def originals():
    manifest = json.loads((ROOT/'sources.json').read_text())
    urls = {}
    for url, meta in manifest.items():
        raw = gzip.decompress((ROOT/meta['file']).read_bytes())
        assert hashlib.sha256(raw).hexdigest() == meta['sha256'] and len(raw) == meta['bytes']
        urls[url] = raw
    return (urls[f'https://data.sec.gov/api/xbrl/companyfacts/CIK{CIK}.json'],
            urls[f'https://data.sec.gov/submissions/CIK{CIK}.json'], urls)


@pytest.fixture(scope='module')
def acquired():
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from app.models import InsightsSnapshot
    from app.services import sec_financial_discovery_store as store, sec_financial_discovery_reader as reader
    facts, company, urls = originals()
    class Client:
        def __init__(self): self.calls = []
        def get(self, url): self.calls.append(url); return urls[url]
    engine = create_engine('sqlite://'); InsightsSnapshot.__table__.create(engine)
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(socket.socket, 'connect', lambda *a, **k: pytest.fail('Unexpected network'))
        patch.setattr(store, 'SessionLocal', sessionmaker(bind=engine))
        patch.setenv('SEC_FINANCIAL_DISCOVERY_ENABLED', '1')
        patch.setenv('SEC_FINANCIAL_DISCOVERY_READ_ENABLED', '1')
        args = dict(symbol='ADAG', cik=CIK, observed_at=NOW, available_by=NOW.date().replace(day=30))
        for _ in range(20):
            client = Client(); result = store.progress(company, facts, client=client, **args)
            assert len(client.calls) <= 6
            if result['phase'] == 'complete': break
        assert result['status'] == 'ready_for_original_revalidation'
        checkpoint = store.read('ADAG', CIK)
        supplement = reader.restore(company, facts, checkpoint=checkpoint, client=Client(), **args)
        assert reader.restore(company, facts, checkpoint=checkpoint, client=Client(), **args) == supplement
        assert reader.prepared_choice(company, facts, client=Client(), **args) == supplement
    engine.dispose()
    return facts, company, supplement


def project(acquired):
    facts, company, supplement = acquired
    return sec.project_fundamentals(facts, company, symbol='ADAG', cik=CIK, observed_at=NOW,
                                   filing_supplement=supplement)


def test_complete_source_partial_values_and_exact_repeat(acquired):
    value = project(acquired); proof = json.loads(value['source_evidence_json'])
    assert {k: value[k] for k in proof['calculations']} == EXPECTED
    assert project(acquired) == value
    assert proof['standard_partial_policy'] == 'qualified_us_gaap_partial_financials_v1'
    assert proof['current']['revenue'] is None and proof['current']['capex'] is None
    assert 'free_cash_flow' not in value and 'revenue_growth' not in value
    assert proof['financial_status'] == 'partial'
    assert proof['standard_foreign_reporting'] == proof['inline_filing_supplement']['standard_foreign_reporting']
    assert tagged.cached_currency_valid(proof)
    assert proof['partial_financial_basis']['accession'] == '0001104659-26-094468'
    assert proof['current']['net_income']['method'] == 'annual_plus_ytd_minus_prior_ytd'
    assert proof['current']['net_income']['value'] == -17609413-16378832+13477240


@pytest.mark.parametrize('defect', ['absent', 'hash', 'currency', 'namespace', 'cik', 'accession', 'filed', 'period', 'cover', 'selection'])
def test_original_binding_cannot_be_bypassed_when_revenue_is_missing(acquired, defect):
    facts, company, supplement = deepcopy(acquired)
    proof = supplement['evidence']
    if defect == 'absent': supplement = None
    elif defect == 'hash': proof['sha256'] = 'invalid'
    elif defect == 'currency': proof['currency'] = 'EUR'
    elif defect == 'namespace': proof['namespace'] = 'ifrs-full'
    elif defect == 'cik': proof['cik'] = '0000000001'
    elif defect == 'accession': proof['accession'] = '0000000001-26-000001'
    elif defect == 'filed': proof['filed'] = '2026-08-13'
    elif defect == 'period': proof['report_date'] = '2026-03-31'
    elif defect == 'cover': proof['cover_evidence']['source_url'] = 'https://example.test/not-sec'
    else: proof['standard_foreign_reporting']['current']['report_date'] = '2026-03-31'
    with pytest.raises((sec.SecFundamentalsError, tagged.TaggedForeignError)): project((facts, company, supplement))


def test_unqualified_foreign_facts_do_not_enable_partial_fallback():
    from test_sec_tagged_foreign import synthetic
    facts, company = synthetic()
    facts['facts']['us-gaap'].pop('RevenueFromContractWithCustomerExcludingAssessedTax')
    with pytest.raises((sec.SecFundamentalsError, tagged.TaggedForeignError)):
        sec.project_fundamentals(json.dumps(facts).encode(), json.dumps(company).encode(),
                                symbol='GENERIC', cik='1', observed_at=NOW)


@pytest.mark.parametrize('metric', ['current_ratio', 'roe'])
def test_complete_source_score_alert_recovery_repeat_crossing_and_rollback(acquired, metric, monkeypatch):
    import test_sec_tagged_foreign_consumers as consumers
    from app.services import sec_inline_financials as inline
    facts, company, supplement = acquired
    monkeypatch.setattr(consumers, 'originals', lambda symbol: (facts, company, CIK, {}))
    monkeypatch.setattr(consumers, 'EXPECTED', {'ADAG': {'metrics': EXPECTED}})
    monkeypatch.setattr(consumers, 'NOW', NOW)
    monkeypatch.setattr(inline, 'acquire', lambda *a, **k: deepcopy(supplement))
    consumers.test_original_source_score_and_alert_recovery_repeat_crossing_rollback('ADAG', metric, monkeypatch)


def test_preparation_persistence_and_cached_repeat_without_canonical_writes(acquired, monkeypatch):
    from sqlalchemy import create_engine, select, func
    from sqlalchemy.orm import sessionmaker
    from app.db import Base
    from app.models import Event, EmailDelivery, FundamentalsCache
    from app.services import sec_fundamentals_preparation as prep, sec_inline_financials as inline
    _, _, urls = originals()
    calls = []
    class Client:
        issuer_document_urls = frozenset()
        def get(self, url): calls.append(url); return urls[url]
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None): return NOW
    engine = create_engine('sqlite://'); Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine)
    monkeypatch.setattr(socket.socket, 'connect', lambda *a, **k: pytest.fail('Unexpected network'))
    monkeypatch.setenv('SEC_FUNDAMENTALS_WARMING_ENABLED', '1')
    monkeypatch.setattr(prep, 'SessionLocal', factory)
    monkeypatch.setattr(prep, 'datetime', Clock)
    monkeypatch.setattr(prep, 'DirectSourceClient', Client)
    monkeypatch.setattr(prep.sec_directory, 'directory', lambda: {'ADAG': {'cik': CIK}})
    monkeypatch.setattr(inline, 'acquire', lambda *a, **k: deepcopy(acquired[2]))
    payload = prep.prepare('ADAG')
    assert payload['values'] == EXPECTED and payload['status'] == 'partial'
    assert payload['calculation_evidence'] == json.loads(project(acquired)['source_evidence_json'])
    assert len(calls) == 2
    assert prep.prepare('ADAG') == payload and len(calls) == 2
    with factory() as db:
        assert all(db.scalar(select(func.count()).select_from(model)) == 0 for model in (Event, EmailDelivery, FundamentalsCache))
    engine.dispose()
