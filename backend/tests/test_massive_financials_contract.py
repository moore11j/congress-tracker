from copy import deepcopy
import importlib.util
import json
from pathlib import Path
import socket
import subprocess
import sys

import pytest

PATH = Path(__file__).parents[1] / 'scripts' / 'audit_massive_financials.py'
spec = importlib.util.spec_from_file_location('financials_audit', PATH)
audit = importlib.util.module_from_spec(spec)
spec.loader.exec_module(audit)


def ratio(**changes):
    return dict(cik='320193', ticker='AAPL', date='2026-10-26', current=0,
                return_on_equity=-0.2, ev_to_ebitda=12, price_to_earnings=20,
                earnings_per_share=6.57, free_cash_flow=100, **changes)


def statement(**changes):
    return dict(cik='0000320193', tickers=['AAPL'], period_end='2026-06-30',
                filing_date='2026-08-01', fiscal_year=2026, fiscal_quarter=2,
                timeframe='quarterly', **changes)


def normalize(rows, kind='ratios', **changes):
    raw = {'status': 'OK', 'results': rows, **changes}
    return audit.normalize(raw, kind, symbol='AAPL', cik='0000320193', observed_date='2026-10-27')


def test_candidate_units_and_non_equivalent_eps_are_separate():
    raw = ratio()
    before = deepcopy(raw)
    result = normalize([raw, raw])
    item, = result['items']
    assert item['candidateValues'] == {'current_ratio': 0, 'roe': -20,
                                      'ev_to_ebitda': 12, 'trailing_pe': 20}
    assert 'eps_ttm' not in item['candidateValues']
    assert item['providerValues']['earnings_per_share'] == 6.57
    assert item['financialPeriod'] is None and not item['stale']
    assert result['currency'] is None and result['publicationEligible'] is False
    assert normalize([raw, raw]) == result and raw == before


@pytest.mark.parametrize('kind', ['income-statements', 'balance-sheets', 'cash-flow-statements'])
def test_statement_values_keep_their_actual_basis(kind):
    row = statement(consolidated_net_income_loss=10,
                    net_income_loss_attributable_common_shareholders=8,
                    total_equity=50, total_equity_attributable_to_parent=40,
                    purchase_of_property_plant_and_equipment=-3,
                    net_cash_from_operating_activities=7)
    result = normalize([row], kind)
    item, = result['items']
    assert item['providerValues']['purchase_of_property_plant_and_equipment'] == -3
    assert item['providerValues']['consolidated_net_income_loss'] == 10
    assert item['providerValues']['net_income_loss_attributable_common_shareholders'] == 8
    assert item['originalAvailabilityDate'] is None
    assert 'candidateValues' not in item
    assert item['latestReportingFilingDate'] == '2026-08-01'


@pytest.mark.parametrize('field,value', [
    ('cik', '1'), ('cik', None), ('ticker', 'MSFT'), ('date', '2026-10-28'),
    ('date', '2026-02-30'), ('date', '20261026'), ('current', True),
    ('current', '1'), ('current', float('nan')), ('current', float('inf')),
    ('return_on_equity', 1e308),
])
def test_invalid_identity_dates_and_numbers_fail(field, value):
    row = ratio()
    row[field] = value
    with pytest.raises(audit.ContractError):
        normalize([row])


@pytest.mark.parametrize('changes', [
    {'next_url': 'https://api.massive.com/next?apiKey=DO_NOT_ECHO'},
    {'status': 'NOT_AUTHORIZED'}, {'error': 'denied'}, {'results': {}},
    {'results': [ratio()] * 101},
])
def test_partial_or_denied_payload_cannot_look_complete(changes):
    with pytest.raises(audit.ContractError) as error:
        normalize([], **changes)
    assert 'DO_NOT_ECHO' not in str(error.value)


@pytest.mark.parametrize('field,value', [
    ('filing_date', '2026-06-01'), ('filing_date', '2026-11-01'),
    ('period_end', '2026-02-30'), ('timeframe', 'monthly'), ('tickers', ['MSFT']),
    ('tickers', 'AAPL'), ('fiscal_year', True), ('fiscal_quarter', 0),
    ('fiscal_quarter', None),
])
def test_statement_identity_and_period_guards(field, value):
    row = statement()
    row[field] = value
    with pytest.raises(audit.ContractError):
        normalize([row], 'income-statements')


def test_duplicates_conflict_and_snapshot_cardinality():
    first, second = ratio(), ratio()
    second['current'] = 1
    with pytest.raises(audit.ContractError, match='conflicting_period'):
        normalize([first, second])
    second['date'] = '2026-10-23'
    with pytest.raises(audit.ContractError, match='multiple_current'):
        normalize([first, second])


def test_stale_null_empty_and_missing_endpoint_are_explicit():
    row = ratio()
    row.update(date='2026-10-01', current=None)
    item, = normalize([row])['items']
    assert item['stale'] and 'current_ratio' not in item['candidateValues']
    result = audit.audit({'symbol': 'AAPL', 'cik': '320193', 'observed_date': '2026-10-27',
                          'responses': {'ratios': {'status': 'OK', 'results': []}}})
    assert result['endpoints']['ratios']['status'] == 'empty'
    assert len(result['missingEndpoints']) == 3 and not result['publicationEligible']


def test_share_class_preserved_and_ttm_balance_rejected():
    row = ratio()
    row['ticker'] = 'BRK.B'
    result = audit.normalize({'status': 'OK', 'results': [row]}, 'ratios',
                             symbol='BRK-B', cik='320193', observed_date='2026-10-27')
    assert result['symbol'] == 'BRK-B'
    row = statement()
    row['timeframe'] = 'trailing_twelve_months'
    with pytest.raises(audit.ContractError, match='timeframe'):
        normalize([row], 'balance-sheets')


def test_audit_imports_no_application_database_and_makes_no_network(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail('unexpected network')
    monkeypatch.setattr(socket, 'socket', forbidden)
    result = normalize([ratio()])
    assert not result['publicationEligible']
    assert 'app.db' not in audit.__dict__


def test_cli_roundtrip_and_duplicate_key_refusal(tmp_path):
    path = tmp_path / 'response.json'
    path.write_text(json.dumps({'symbol': 'AAPL', 'cik': '320193', 'observed_date': '2026-10-27',
                    'responses': {'ratios': {'status': 'OK', 'results': [ratio()]}}}))
    done = subprocess.run([sys.executable, str(PATH), str(path)], capture_output=True, text=True, timeout=10)
    assert done.returncode == 0, done.stderr
    result = json.loads(done.stdout)
    assert result['networkRequests'] == result['databaseWrites'] == 0
    assert not result['publicationEligible']
    path.write_text('{"symbol":"AAPL","symbol":"MSFT"}')
    failed = subprocess.run([sys.executable, str(PATH), str(path)], capture_output=True, text=True, timeout=10)
    assert failed.returncode != 0 and 'duplicate_json_key' in failed.stderr
