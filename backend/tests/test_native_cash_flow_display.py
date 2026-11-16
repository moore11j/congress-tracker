from copy import deepcopy
from datetime import date, datetime, timezone
import gzip
import json
from pathlib import Path
import socket
from types import SimpleNamespace

import pytest

from app.services.fundamentals_cache import fundamentals_summary_from_cache_row
from app.services import sec_nvo_financials
from test_sec_currency_ratios import project as project_enb


def summary(values):
    values = dict(values)
    values['period_date'] = date.fromisoformat(str(values['period_date']))
    return fundamentals_summary_from_cache_row(SimpleNamespace(**values), now=values['fetched_at'])


@pytest.fixture
def enb():
    return project_enb()


def test_actual_cad_projection_retains_currency_and_all_scored_contracts(enb, monkeypatch):
    monkeypatch.setattr(socket.socket, 'connect', lambda *a, **k: pytest.fail('Display attempted network'))
    original = deepcopy(enb)
    displayed = summary(enb)
    from app.services.confirmation_score import confirmation_score_bundle_from_source_contexts
    score_before = confirmation_score_bundle_from_source_contexts('ENB', lookback_days=30,
        source_contexts={'fundamentals': displayed})
    assert displayed['context']['free_cash_flow'] is None
    assert displayed['context']['free_cash_flow_display'] == {
        'value': 1_668_000_000, 'currency': 'CAD', 'period_date': '2026-06-30', 'period_basis': 'TTM',
        'basis': 'TTM operating cash flow minus TTM cash paid for property, plant and equipment',
    }
    without = deepcopy(enb)
    evidence = json.loads(without['source_evidence_json'])
    evidence.pop('native_cash_flow')
    without['source_evidence_json'] = json.dumps(evidence)
    display_only = displayed['context'].pop('free_cash_flow_display')
    assert displayed == summary(without)
    assert score_before == confirmation_score_bundle_from_source_contexts('ENB', lookback_days=30,
        source_contexts={'fundamentals': summary(without)})
    assert enb == original and 'free_cash_flow' not in enb
    assert display_only['currency'] == 'CAD'


def test_actual_dkk_originals_preserve_reviewed_cash_definition(monkeypatch):
    monkeypatch.setattr(socket.socket, 'connect', lambda *a, **k: pytest.fail('Display attempted network'))
    root = Path(__file__).parent/'fixtures/sec_nvo'
    originals = {k: gzip.decompress((root/(k+'.htm.gz')).read_bytes()) for k in sec_nvo_financials.SOURCES}
    company = gzip.decompress((root/'company.json.gz').read_bytes())
    values = sec_nvo_financials.project(b'{"cik":353278}', company, symbol='NVO', cik=sec_nvo_financials.CIK,
        observed_at=datetime(2026,10,16,2,30,tzinfo=timezone.utc), supplement={'nvo_originals': originals})
    result = summary(values)['context']
    assert result['free_cash_flow'] is None
    assert result['free_cash_flow_display']['value'] == 77_859_000_000
    assert result['free_cash_flow_display']['currency'] == 'DKK'
    assert result['free_cash_flow_display']['basis'] == 'TTM CFO excluding interest received/paid minus PPE purchases'
    assert 'free_cash_flow' not in values and 'ev_to_ebitda' not in values


@pytest.mark.parametrize('case', ['currency_mismatch','unknown_currency','usd','period_mismatch','no_period',
    'missing_amount','null_amount','boolean','infinite','conflicting_keys','missing_basis','invalid_json','bad_shape','fmp'])
def test_incomplete_or_conflicting_display_evidence_is_not_guessed(enb, case):
    evidence = json.loads(enb['source_evidence_json'])
    native = evidence['native_cash_flow']
    if case == 'currency_mismatch': native['currency'] = 'EUR'
    if case == 'unknown_currency': native['currency'] = evidence['reporting_currency'] = 'ZZZ'
    if case == 'usd': native['currency'] = evidence['reporting_currency'] = 'USD'
    if case == 'period_mismatch': evidence['period_end'] = '2025-06-30'
    if case == 'no_period': evidence.pop('period_end')
    if case == 'missing_amount': native.pop('current')
    if case == 'null_amount': native['current'] = None
    if case == 'boolean': native['current'] = True
    if case == 'infinite': native['current'] = float('inf')
    if case == 'conflicting_keys': native['value'] = native['current'] + 1
    if case == 'missing_basis': native.pop('formula')
    if case == 'bad_shape': evidence['native_cash_flow'] = []
    if case == 'fmp': enb['provider'] = 'fmp'
    enb['source_evidence_json'] = '{' if case == 'invalid_json' else json.dumps(evidence)
    assert 'free_cash_flow_display' not in summary(enb)['context']


@pytest.mark.parametrize('amount', [0, -123_000_000])
def test_zero_and_negative_reported_amounts_are_not_missing(enb, amount):
    evidence = json.loads(enb['source_evidence_json'])
    evidence['native_cash_flow']['current'] = amount
    enb['source_evidence_json'] = json.dumps(evidence)
    assert summary(enb)['context']['free_cash_flow_display']['value'] == amount


def test_existing_canonical_cash_wins_without_altering_its_value(enb):
    enb['free_cash_flow'] = 17
    result = summary(enb)['context']
    assert result['free_cash_flow'] == 17
    assert 'free_cash_flow_display' not in result


def test_combined_source_and_public_projection_preserve_display_only_context(enb):
    import app.main as main
    enb['provider'] = 'free_direct'
    before = summary(enb)
    public = main._ticker_context_bundle_public_payload(before)
    assert public['context']['free_cash_flow_display'] == before['context']['free_cash_flow_display']
    assert public['context']['free_cash_flow'] is None
    assert 'provider' not in public['context']
    assert public['metrics'] == before['metrics']
    assert public['status'] == before['status']
