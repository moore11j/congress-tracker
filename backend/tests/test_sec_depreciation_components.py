from copy import deepcopy
from datetime import date
import pytest
from app.services import sec_depreciation_components as da

KEY = (date(2025, 7, 1), date(2026, 6, 30))


def records():
    base = dict(start=str(KEY[0]), end=str(KEY[1]), filed='2026-07-29',
                accession=da.ACCESSION, unit='USD', source_cik=da.CIK)
    return {'leverage_da': {},
            'da_depreciation': {KEY: dict(base, tag='Depreciation', value=34.3e9)},
            'da_intangible_amortization': {KEY: dict(base, tag='AmortizationOfIntangibleAssets', value=4.7e9)}}


def apply(rows):
    return da.reconcile(rows, symbol='MSFT', cik=da.CIK)


def test_reviewed_component_sum_preserves_input_evidence_and_repeat():
    rows = records(); saved = deepcopy(rows)
    evidence = apply(rows)
    fact = evidence['periods'][0]['component_sum']
    assert fact['value'] == 39e9
    assert fact['inputs'] == [rows[name][KEY] for name in ('da_depreciation','da_intangible_amortization')]
    assert fact['proof_sha256'] == da.PROOF_SHA256
    assert [p['period_end'] for p in evidence['periods']] == ['2026-06-30']
    assert evidence['status'] == 'reconciliation_required'
    assert not rows['leverage_da']
    assert evidence == apply(rows)
    assert rows == saved
    fact['inputs'][0]['value'] = 0
    assert rows == saved


@pytest.mark.parametrize('symbol,cik', [('OTHER', da.CIK), ('MSFT', '0000000001')])
def test_other_issuers_are_unchanged(symbol, cik):
    rows = records()
    saved=deepcopy(rows)
    evidence = da.reconcile(rows, symbol=symbol, cik=cik)
    assert evidence is None and rows == saved


@pytest.mark.parametrize('existing', [None, {'value': 45e9}])
def test_reported_total_or_conflict_never_replaced(existing):
    rows = records(); rows['leverage_da'][KEY] = existing
    evidence = apply(rows)
    assert rows['leverage_da'][KEY] == existing
    assert evidence['periods'][0]['reported_total'] == existing
    assert evidence['periods'][0]['reported_total_conflicted'] == (existing is None)


@pytest.mark.parametrize('field,value', [('accession','later-filing'), ('unit','EUR'),
    ('source_cik','0000000001'), ('start','2025-07-02'), ('end','2026-06-29'),
    ('filed','2026-07-30'), ('value',-1), ('value',float('inf')), ('value',float('nan')),
    ('value',True), ('value','4700000000')])
def test_ambiguous_or_invalid_component_is_held(field, value):
    rows=records(); rows['da_intangible_amortization'][KEY][field]=value
    evidence=apply(rows)
    assert not evidence['periods']
    assert KEY not in rows['leverage_da']
    assert '2026-06-30' in evidence['holds']


@pytest.mark.parametrize('kind', ['missing', 'conflict'])
def test_missing_component_is_never_zero(kind):
    rows=records()
    if kind == 'missing': rows['da_intangible_amortization'].clear()
    else: rows['da_intangible_amortization'][KEY]=None
    evidence=apply(rows)
    assert not evidence['periods']
    assert KEY not in rows['leverage_da']


def test_explicit_zero_is_preserved():
    rows=records(); rows['da_intangible_amortization'][KEY]['value']=0
    evidence=apply(rows)
    assert evidence['periods'][0]['component_sum']['value']==34.3e9
    assert not rows['leverage_da']


def test_future_period_requires_review_even_with_same_tags():
    rows=records(); future=(date(2026,7,1),date(2027,6,30))
    for name in ('da_depreciation','da_intangible_amortization'):
        point=rows[name].pop(KEY)
        point.update(start=str(future[0]),end=str(future[1]))
        rows[name][future]=point
    evidence=apply(rows)
    assert future not in rows['leverage_da']
    assert not evidence['periods']


def test_only_reviewed_cache_version_changes():
    from app.services.sec_fundamentals_preparation import VERSION, version_for_symbol
    assert version_for_symbol('MSFT') == VERSION+'_'+da.VERSION
    assert version_for_symbol('NVDA') == VERSION
    assert 'sec_arm_usd_reporting' in version_for_symbol('ARM')


def test_lease_amortization_is_retained_but_never_added_to_component_sum():
    rows=records()
    rows['da_finance_lease_amortization']={KEY: dict(rows['da_depreciation'][KEY], value=5.403e9, tag='FinanceLeaseRightOfUseAssetAmortization')}
    evidence=apply(rows)
    period=evidence['periods'][0]
    assert period['component_sum']['value']==39e9
    assert period['reported_supplements']['da_finance_lease_amortization']['value']==5.403e9
    assert not rows['leverage_da']


def test_other_period_supplement_is_not_attached_to_reviewed_sum():
    rows=records()
    rows['da_cash_flow_adjustment']={KEY:dict(rows['da_depreciation'][KEY], accession='other', value=38.534e9)}
    assert not apply(rows)['periods'][0]['reported_supplements']
