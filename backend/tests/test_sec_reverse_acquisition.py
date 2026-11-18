from copy import deepcopy
from datetime import date, datetime, timezone
from functools import lru_cache
from pathlib import Path
from types import SimpleNamespace
import gzip
import hashlib
import json
import socket
from unittest.mock import patch

import pytest

from app.services import sec_reverse_acquisition as reverse
from app.services import sec_fundamentals as sec, sec_inline_financials as inline
from app.services import sec_financial_statements as statements, sec_common_income as common
from app.services import sec_tagged_foreign

ROOT = Path(__file__).parent / 'fixtures/sec_reverse_acquisition'
NOW = datetime(2026, 11, 17, 4, tzinfo=timezone.utc)


@lru_cache(maxsize=5)
def raw(name):
    source = json.loads((ROOT/'sources.json').read_bytes())[name]
    value = gzip.decompress((ROOT/source['file']).read_bytes())
    assert len(value) == source['bytes'] and hashlib.sha256(value).hexdigest() == source['sha256']
    return value


class Client:
    def __init__(self):
        self.calls = []
        self.names = {p['url']: name for name, p in json.loads((ROOT/'sources.json').read_bytes()).items()}
    def get(self, url):
        self.calls.append(url)
        assert url in self.names, 'Unretained source requested'
        return raw(self.names[url])


def acquire(client=None, company=None):
    return inline.acquire(raw('VMRK-facts.json'), company or raw('VMRK-company.json'),
        symbol='VMRK', cik=reverse.CIK, observed_at=NOW, client=client or Client())


@lru_cache(maxsize=1)
def acquired():
    return acquire()


def project(source=None, facts=None, company=None, **kwargs):
    args = dict(symbol='VMRK', cik=reverse.CIK, observed_at=NOW, filing_supplement=acquired() if source is None else source)
    args.update(kwargs)
    return sec.project_fundamentals(facts or raw('VMRK-facts.json'), company or raw('VMRK-company.json'), **args)


def test_complete_originals_replace_mixed_legal_history_and_keep_sector_holds():
    from app.services.fundamentals_cache import fundamentals_summary_from_cache_row
    with patch.object(socket.socket, 'connect', side_effect=AssertionError('No network')):
        client = Client(); source = acquire(client); value = project(source)
        assert project(source) == value
    assert len(client.calls) == len(set(client.calls)) == 3
    evidence = json.loads(value['source_evidence_json'])
    assert evidence['current']['revenue']['value'] == 3_511_386_000
    assert evidence['current']['net_income']['value'] == -88_966_000
    assert evidence['current']['operating_cash_flow']['value'] == 1_442_305_000
    assert value['net_margin'] == pytest.approx(-2.5336434103228753)
    assert value['revenue_growth'] == pytest.approx(16.525022416405562)
    assert value['roe'] == pytest.approx(-0.3722803186355641)
    assert value['roe'] != -0.08596955221592613
    for name in ('free_cash_flow', 'fcf_margin', 'fcf_growth', 'operating_margin', 'gross_margin', 'debt_to_ebitda'):
        assert value.get(name) is None
    summary = fundamentals_summary_from_cache_row(SimpleNamespace(**value), now=NOW)
    assert summary['status'] == 'unavailable' and not summary['data_quality']['available']
    assert summary['context']['source_label'] == 'SEC EDGAR'
    assert reverse.cached_valid(evidence) and sec_tagged_foreign.cached_currency_valid(evidence)
    assert len(evidence['financial_continuity']['overlaps']) == 7
    for metric in ('revenue', 'net_income', 'operating_cash_flow'):
        points = evidence['current'][metric]['inputs']
        assert {p['source_cik'] for p in points} == {reverse.CIK, reverse.PREDECESSOR_CIK}
        assert all(p['accounting_basis_cik'] == reverse.PREDECESSOR_CIK for p in points)
        assert not any(p['accession'] == '0001193125-26-051433' for p in points)


def test_statement_panel_and_common_equity_use_same_accounting_predecessor():
    value = project(); evidence = json.loads(value['source_evidence_json'])
    args = dict(symbol='VMRK', cik=reverse.CIK, observed_at=NOW, filing_supplement=acquired())
    result = common.resolve(raw('VMRK-facts.json'), raw('VMRK-company.json'), evidence, **args)
    assert result['status'] == 'available'
    assert result['point']['value'] == -89_321_000
    assert result['point']['value'] == evidence['current']['net_income']['value'] - 355_000
    panel = statements.project(raw('VMRK-facts.json'), raw('VMRK-company.json'), **args)
    assert panel['summary']['revenueTtm'] == 3_511_386_000
    assert panel['summary']['netIncomeTtm'] == -88_966_000
    assert panel == statements.project(raw('VMRK-facts.json'), raw('VMRK-company.json'), **args)
    assert panel['sourceEvidence']['financialContinuity'] == evidence['financial_continuity']
    assert 'AvalonBay' in panel['reportingBasis']
    assert len(panel['annual']) == 3
    annual = {r['date']: r for r in panel['annual']}
    assert annual['2025-12-31']['revenue'] == 3_040_725_000
    assert annual['2025-12-31']['netIncome'] == 1_051_301_000
    for row in panel['annual'] + panel['quarterly']:
        assert row.get('eps') is None and row.get('freeCashFlow') is None
    cache = SimpleNamespace(source='sec_edgar', fetched_at=NOW, payload_json=json.dumps(panel))
    assert statements.cached_row_payload(cache, 'VMRK', now=NOW) == panel
    panel['sourceEvidence'].pop('financialContinuity')
    cache.payload_json = json.dumps(panel)
    assert statements.cached_row_payload(cache, 'VMRK', now=NOW) is None


@pytest.mark.parametrize('name', ['current', 'annual', 'comparative'])
@pytest.mark.parametrize('defect', ['missing', 'changed', 'wrong_original'])
def test_missing_or_changed_original_never_falls_back_to_legal_companyfacts(name, defect):
    source = deepcopy(acquired())
    if defect == 'missing': source['reverse_acquisition'].pop(name)
    elif defect == 'changed': source['reverse_acquisition'][name] += b' '
    else: source['reverse_acquisition'][name] = source['reverse_acquisition']['annual' if name == 'current' else 'current']
    with pytest.raises(sec.SecFundamentalsCoverageError, match='reverse_acquisition_'):
        project(source)


@pytest.mark.parametrize('defect', ['future', 'amendment', 'transition', 'foreign_annual', 'ambiguous', 'wrong_cik', 'wrong_symbol', 'missing_directory'])
def test_unreviewed_identity_or_latest_filing_is_held_before_source_requests(defect):
    company = json.loads(raw('VMRK-company.json')); recent = company['filings']['recent']
    index = recent['accessionNumber'].index('0001193125-26-453735')
    if defect == 'future': recent['reportDate'][index] = '2026-10-31'; recent['filingDate'][index] = '2026-11-12'
    elif defect == 'amendment': recent['form'][index] = '10-Q/A'
    elif defect == 'transition': recent['form'][index] = '10-QT'
    elif defect == 'foreign_annual': recent['form'][index] = '20-F'
    elif defect == 'ambiguous':
        for column in ('accessionNumber', 'form', 'filingDate', 'reportDate', 'primaryDocument'):
            recent[column].append(recent[column][index])
    elif defect == 'wrong_cik': company['cik'] = '1'
    elif defect == 'wrong_symbol': company['tickers'] = ['EQR']
    else: company['filings'] = {}
    encoded = json.dumps(company).encode(); client = Client(); source = acquire(client, encoded)
    assert not client.calls and 'reverse_acquisition_error' in source
    with pytest.raises(sec.SecFundamentalsError): project(source, company=encoded)


def test_unavailable_proof_or_naive_observation_cannot_emit_mixed_ratios():
    for source in ({}, {'evidence': {}}, {'reverse_acquisition_error': 'changed'}):
        with pytest.raises(sec.SecFundamentalsCoverageError): project(source)
    with pytest.raises((ValueError, sec.SecFundamentalsCoverageError)):
        project(observed_at=NOW.replace(tzinfo=None))
    with pytest.raises(sec.SecFundamentalsCoverageError):
        project(history_raw=raw('VMRK-facts.json'))


def test_same_filing_conflict_is_held_but_old_legal_history_cannot_contaminate():
    payload = json.loads(raw('VMRK-facts.json'))
    points = payload['facts']['us-gaap']['NetIncomeLoss']['units']['USD']
    for point in points:
        if point['accn'] != '0001193125-26-453735': point['val'] = 123456789
    assert project(facts=json.dumps(payload).encode())['roe'] == project()['roe']
    points.append(dict(start='2026-01-01', end='2026-09-30', val=-253698999,
        filed='2026-11-09', form='10-Q', accn='0001193125-26-453735'))
    with pytest.raises(sec.SecFundamentalsCoverageError, match='fact_conflict'):
        project(facts=json.dumps(payload).encode())


@pytest.mark.parametrize('defect', ['missing', 'source_hash', 'predecessor', 'overlap', 'methodology'])
def test_cached_accounting_proof_must_remain_reviewed(defect):
    evidence = json.loads(project()['source_evidence_json'])
    proof = evidence['financial_continuity']
    if defect == 'missing': evidence.pop('financial_continuity')
    elif defect == 'source_hash': proof['sources']['annual']['sha256'] = '0'*64
    elif defect == 'predecessor': proof['predecessor_cik'] = reverse.CIK
    elif defect == 'overlap': proof['overlaps'][0]['value'] += 1
    else: proof['methodology'] = 'unreviewed'
    assert not sec_tagged_foreign.cached_currency_valid(evidence)


def test_other_issuers_and_historical_dates_are_outside_review():
    assert not reverse.applies(reverse.PREDECESSOR_CIK, date(2026, 11, 16))
    assert not reverse.applies('1', date(2026, 11, 16))
    assert not reverse.applies(reverse.CIK, date(2026, 11, 8))
    assert reverse.applies(reverse.CIK, date(2026, 11, 9))
    assert sec_tagged_foreign.cached_currency_valid({'cik': '0000000001'})
    assert not sec_tagged_foreign.cached_currency_valid({'cik': reverse.CIK})
    assert sec_tagged_foreign.cached_currency_valid({'cik': reverse.CIK, 'available_by': '2026-11-08'})
    assert not sec_tagged_foreign.cached_currency_valid({'cik': reverse.CIK, 'available_by': '2026-11-09'})


def test_vendor_fallback_cannot_restore_unverified_accounting_basis():
    from app.services import finnhub_metrics, free_fundamentals
    from test_finnhub_metric_identity import payload
    original = project()
    metrics = finnhub_metrics.normalize(payload('VMRK'), symbol='VMRK', observed_at=NOW)
    assert metrics['values']
    combined = free_fundamentals.combine(original, metrics)
    for key, value in original.items():
        if key not in {'provider', 'source_evidence_json'}:
            assert combined[key] == value
    assert combined.get('operating_margin') is None and combined.get('trailing_pe') is None
    evidence = json.loads(combined['source_evidence_json'])
    assert not evidence['supplement']['used_fields']
    assert evidence['supplement']['hold_reason'] == 'unverified_reverse_acquisition_accounting_basis'


@pytest.mark.parametrize('delivery', [False, True])
def test_ordinary_preparation_rejects_legacy_cache_and_repeats_without_requests(monkeypatch, delivery):
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from app.db import Base
    from app.models import InsightsSnapshot
    from app.services import sec_fundamentals_preparation as prep, sec_directory
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine, tables=[InsightsSnapshot.__table__])
    factory = sessionmaker(bind=engine)
    monkeypatch.setattr(prep, 'SessionLocal', factory)
    monkeypatch.setattr(sec_directory, 'directory', lambda: {'VMRK': {'cik': reverse.CIK}})
    monkeypatch.setenv('SEC_FUNDAMENTALS_WARMING_ENABLED', '1')
    monkeypatch.setenv('SEC_FINANCIAL_DISCOVERY_ENABLED', '0')
    if delivery:
        from test_sec_reverse_acquisition_delivery import DeliveryClient
        client = DeliveryClient()
    else:
        client = Client()
    monkeypatch.setattr(prep, 'DirectSourceClient', lambda: client)
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return NOW
    monkeypatch.setattr(prep, 'datetime', Clock)
    legacy = {'version': prep.version_for_symbol('VMRK'), 'symbol': 'VMRK', 'source': 'sec_edgar',
        'observed_at': NOW.isoformat(), 'status': 'partial', 'values': {'roe': -0.08596955221592613},
        'calculation_evidence': {'cik': reverse.CIK, 'methodology': 'sec_independent_financials_v3'}}
    if delivery:
        legacy.update(version=prep.VERSION, status='unavailable', values={},
                      reason='unsupported_current_financial_coverage', detail='reverse_acquisition_originals_required')
    with factory() as db:
        db.add(InsightsSnapshot(kind='sec-current-fundamentals:VMRK:v1', source='sec_edgar',
            fetched_at=NOW, payload_json=json.dumps(legacy)))
        db.commit()
    assert prep._cached('VMRK', NOW) is None
    result = prep.prepare('VMRK')
    assert result['values']['roe'] == -0.3722803186355641
    assert set(result['values']) == {'roe', 'net_margin', 'revenue_growth'}
    assert len(client.calls) == len(set(client.calls)) == 5
    client.calls.clear()
    assert prep.prepare('VMRK') == result and not client.calls
    engine.dispose()
