from copy import deepcopy
from datetime import date, datetime, timezone
import json

import pytest

from app.services import finnhub_metrics as metrics, free_fundamentals as free
from app.services.finnhub_metric_identity import ISSUER_FIELDS, association
from app.services.finnhub_research import FinnhubUnavailable

NOW = datetime(2026, 10, 11, tzinfo=timezone.utc)
CASES = [('GOOG', 'GOOGL', '0001652044'), ('BRK.B', 'BRK.A', '0001067983'), ('BRK-B', 'BRK.A', '0001067983'),
         ('TSM', '2330.TW', '0001046179'), ('CX', 'CEMEXCPO.MX', '0001076378'),
         ('ASML', 'ASML.AS', '0000937966'), ('SAP', 'SAP.DE', '0001000184'),
         ('NVS', 'NOVN.SW', '0001114448'), ('FOX', 'FOXA', '0001754301'),
         ('FWONK', 'FWONA', '0001560385')]


def payload(symbol):
    return {'symbol': symbol, 'metricType': 'all',
        'metric': {key: 12 for key, _ in metrics.SCALARS.values()},
        'series': {'annual': {'ebitda': [{'period':'2025-12-31','v':120}, {'period':'2024-12-31','v':100}]}, 'quarterly': {key: [{'period': '2026-06-30', 'v': .25}]
            for key, _, _ in metrics.SERIES.values()}}}


def sec(symbol, cik):
    return {'symbol': symbol, 'provider': 'sec_edgar', 'period_date': date(2026, 6, 30),
        'roe': 21, 'revenue_growth': 15, 'free_cash_flow': 500,
        'source_evidence_json': json.dumps({'cik': cik, 'market': {'provider': 'massive'}})}


@pytest.mark.parametrize('requested,reported,cik', CASES)
def test_reviewed_pair_is_issuer_only_with_explicit_original_identity(requested, reported, cik):
    raw = payload(reported); original = deepcopy(raw)
    result = metrics.normalize(raw, symbol=requested, observed_at=NOW)
    assert set(result['values']) == ISSUER_FIELDS
    assert result['symbol'] == requested and result['issuer_identity']['source_symbol'] == reported
    assert result['issuer_identity']['cik'] == cik
    assert result['fields']['roe']['period_date'] == '2026-06-30'
    assert result['fields']['net_margin']['period_date'] is None
    assert all(v['identity_basis'] == 'consolidated_issuer' and v['source_symbol'] == reported
               for v in result['fields'].values())
    before = sec(requested, cik); combined = free.combine(before, result)
    assert before == sec(requested, cik) and raw == original
    assert combined['roe'] == 21 and combined['revenue_growth'] == 15
    assert combined['free_cash_flow'] == 500 and combined['current_ratio'] == .25
    assert not set(metrics.SCALARS).difference(ISSUER_FIELDS) & combined.keys()
    evidence = json.loads(combined['source_evidence_json'])
    assert evidence['market'] == {'provider': 'massive'}
    assert evidence['supplement']['issuer_identity'] == result['issuer_identity']


@pytest.mark.parametrize('cik', ['', None, '0000000000', '0001046179', 'True'])
def test_independent_sec_issuer_proof_is_required(cik):
    result = metrics.normalize(payload('BRK.A'), symbol='BRK.B', observed_at=NOW)
    with pytest.raises(FinnhubUnavailable, match='issuer_identity_invalid'):
        free.combine(sec('BRK.B', cik), result)


@pytest.mark.parametrize('requested,reported', [('ABC', 'BRK.A'), ('BRK.B', 'BRK27'),
    ('BRK.B', 'BRK'), ('TSM', 'TSM.TO'), ('CX', 'CEMEX'), ('BRK/B', 'BRK.A'), ('GOOG', 'GOOGM'), ('GOOG', 'GOOGN'), ('GOOGL', 'GOOG')])
def test_unreviewed_pairs_do_not_gain_implicit_alias_or_issuer_permission(requested, reported):
    with pytest.raises(FinnhubUnavailable, match='symbol_mismatch'):
        metrics.normalize(payload(reported), symbol=requested, observed_at=NOW)


@pytest.mark.parametrize('field', ['beta', 'trailing_pe', 'forward_pe', 'price_to_sales', 'ev_to_ebitda', 'eps_growth'])
def test_combiner_refuses_injected_market_and_per_share_fields(field):
    result = metrics.normalize(payload('BRK.A'), symbol='BRK.B', observed_at=NOW)
    result['values'][field] = 10
    with pytest.raises(FinnhubUnavailable, match='issuer_fields_invalid'):
        free.combine(sec('BRK.B', '0001067983'), result)


def test_identity_and_field_provenance_cannot_drift():
    result = metrics.normalize(payload('2330.TW'), symbol='TSM', observed_at=NOW)
    altered = deepcopy(result); altered['issuer_identity']['sources'] = ['https://example.test']
    with pytest.raises(FinnhubUnavailable, match='issuer_identity_invalid'):
        free.combine(sec('TSM', '0001046179'), altered)
    altered = deepcopy(result); altered['fields']['roe']['source_symbol'] = 'TSM'
    with pytest.raises(FinnhubUnavailable, match='issuer_fields_invalid'):
        free.combine(sec('TSM', '0001046179'), altered)
    copied = association('TSM', '2330.TW'); copied['sources'].clear()
    assert association('TSM', '2330.TW')['sources']


def test_mismatch_at_combination_retains_sec_without_using_foreign_metrics(monkeypatch):
    monkeypatch.setattr(free.sec_fundamentals, 'fetch_fundamentals', lambda s: sec(s, '1'))
    monkeypatch.setattr(metrics, 'prepare', lambda s: metrics.normalize(payload('BRK.A'), symbol=s, observed_at=NOW))
    result = free.fetch_fundamentals('BRK.B')
    assert result['roe'] == 21 and result['free_cash_flow'] == 500
    assert 'current_ratio' not in result
    assert json.loads(result['source_evidence_json'])['supplement_error'] == 'metric_issuer_identity_invalid'


def test_recent_dated_financials_still_required_and_newer_sec_period_preserved():
    raw = payload('BRK.A'); raw['series'] = {}
    assert metrics.normalize(raw, symbol='BRK.B', observed_at=NOW)['values'] == {}
    result = metrics.normalize(payload('BRK.A'), symbol='BRK.B', observed_at=NOW)
    newer = sec('BRK.B', '0001067983'); newer['period_date'] = date(2026, 9, 30)
    assert 'current_ratio' not in free.combine(newer, result)


def test_exact_security_retains_all_original_fields_and_no_cross_listing_policy():
    result = metrics.normalize(payload('BRK.A'), symbol='BRK.A', observed_at=NOW)
    assert set(result['values']) == set(metrics.SCALARS) | set(metrics.SERIES) | set(metrics.DERIVED)
    assert 'issuer_identity' not in result
    assert all('source_symbol' not in field for field in result['fields'].values())


@pytest.mark.parametrize('cik', ['0001067983', '', None])
def test_alphabet_financial_association_requires_alphabet_sec_issuer(cik):
    result = metrics.normalize(payload('GOOGL'), symbol='GOOG', observed_at=NOW)
    with pytest.raises(FinnhubUnavailable, match='issuer_identity_invalid'):
        free.combine(sec('GOOG', cik), result)


@pytest.mark.parametrize('requested,reported,cik', CASES[5:])
def test_new_listing_associations_still_require_matching_sec_cik(requested, reported, cik):
    result = metrics.normalize(payload(reported), symbol=requested, observed_at=NOW)
    with pytest.raises(FinnhubUnavailable, match='issuer_identity_invalid'):
        free.combine(sec(requested, '0000000001'), result)


@pytest.mark.parametrize('requested,reported', [
    ('ASML', 'ASMLF'), ('SAP', 'SAPGF'), ('NVS', 'NVSEF'),
    ('FOX', 'FOXB'), ('FWONK', 'LLYVA'), ('FWONK', 'LSXMA'), ('FWONK', 'FWONB'),
])
def test_related_and_tracking_symbols_are_not_automatically_equivalent(requested, reported):
    with pytest.raises(FinnhubUnavailable, match='symbol_mismatch'):
        metrics.normalize(payload(reported), symbol=requested, observed_at=NOW)
