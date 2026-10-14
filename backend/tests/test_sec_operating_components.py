from copy import deepcopy
from datetime import date
import pytest
from app.services import sec_operating_components as subject

PERIOD=(date(2026,4,1),date(2026,6,30))


def records():
    # HCA's actual Q2 2026 consolidated income statement, millions of USD.
    amounts=dict(zip(('revenue',*subject.TAGS),(20230,8290,2886,5043,944,2494,599,16,10)))
    return {name:{PERIOD:{'value':amount*1_000_000,'start':'2026-04-01','end':'2026-06-30',
        'unit':'USD','filed':'2026-07-28','accession':'0001193125-26-321077',
        'tag':'Revenues' if name=='revenue' else subject.TAGS[name][0],'priority':0}}
        for name,amount in amounts.items()} | {'operating_income':{}}


def apply(value):
    return subject.apply(value,symbol='HCA',cik='0000860730')


def test_actual_expenses_reconcile_and_preserve_inputs():
    value=records();before=deepcopy(value);actual,proof=apply(value)
    fact=actual['operating_income'][PERIOD]
    assert fact['value']==3_067_000_000==fact['reconciliation_value']
    assert fact['tag']==subject.TAG and fact['source_cik']==subject.CIK and fact['derived']
    assert len(fact['inputs'])==9 and proof['status']=='derived' and not proof['holds']
    assert value==before and apply(value)==(actual,proof)
    fact['inputs']['revenue']['value']=0
    assert value==before


@pytest.mark.parametrize('missing',('revenue',*subject.TAGS))
def test_missing_is_not_zero(missing):
    value=records();value[missing]={}
    actual,proof=apply(value)
    assert not actual['operating_income']


@pytest.mark.parametrize('field,new',[('unit','EUR'),('accession','another'),('filed','2026-07-29'),
    ('start','2026-01-01'),('end','2026-03-31'),('source_cik','0000320193'),('accession',None),('filed','')])
def test_incompatible_inputs_are_held(field,new):
    value=records();value['hca_supplies'][PERIOD][field]=new
    actual,proof=apply(value)
    assert not actual['operating_income'] and proof['holds'][0]['reason']=='incompatible_components'


@pytest.mark.parametrize('number',[float('nan'),float('inf'),float('-inf'),True,'0',10**400])
def test_invalid_numbers_are_held(number):
    value=records();value['hca_supplies'][PERIOD]['value']=number
    actual,proof=apply(value)
    assert not actual['operating_income'] and proof['holds'][0]['reason']=='nonfinite_component'


@pytest.mark.parametrize('name',('hca_labor','hca_supplies','hca_other','hca_da','hca_interest'))
def test_negative_expense_is_not_silently_normalized(name):
    value=records();value[name][PERIOD]['value']=-1
    actual,proof=apply(value)
    assert not actual['operating_income'] and proof['holds'][0]['reason']=='negative_expense_component'


def test_pretax_reconciliation_catches_new_cost_or_definition():
    value=records();value['hca_pretax'][PERIOD]['value']+=1_000_000
    actual,proof=apply(value)
    assert not actual['operating_income'] and proof['holds'][0]['reason']=='pretax_reconciliation_failed'


def test_explicit_zero_affiliate_and_disposal_are_valid():
    value=records()
    value['hca_affiliates'][PERIOD]['value']=0
    value['hca_disposals'][PERIOD]['value']=0
    value['hca_pretax'][PERIOD]['value']=2_468_000_000
    assert apply(value)[0]['operating_income'][PERIOD]['value']==3_067_000_000


def test_loss_on_disposal_sign_and_actual_prior_quarter():
    value=records()
    amounts=(18605,8138,2844,3793,863,2415,568,19,-3)
    for name,amount in zip(('revenue',*subject.TAGS),amounts):value[name][PERIOD]['value']=amount*1_000_000
    assert apply(value)[0]['operating_income'][PERIOD]['value']==2_967_000_000


@pytest.mark.parametrize('reported',[None,4_000_000_000])
def test_reported_series_or_conflict_prevents_derived_splice(reported):
    value=records();value['operating_income']={PERIOD:reported}
    actual,proof=apply(value)
    assert actual is value and proof['status']=='reported_series_retained'


@pytest.mark.parametrize('symbol,cik',[('OTHER',subject.CIK),('HCA','0000320193')])
def test_only_reviewed_issuer_changes(symbol,cik):
    value=records();actual,proof=subject.apply(value,symbol=symbol,cik=cik)
    assert actual is value and proof is None


def test_periods_before_reviewed_component_history_remain_unavailable():
    value=records();old=(date(2023,4,1),date(2023,6,30))
    for rows in value.values():
        if PERIOD in rows:
            fact=rows.pop(PERIOD);fact.update(start=str(old[0]),end=str(old[1]));rows[old]=fact
    assert not apply(value)[0]['operating_income']


def test_cache_version_changes_only_reviewed_symbol():
    from app.services.sec_fundamentals_preparation import version_for_symbol,VERSION
    assert version_for_symbol('HCA')==VERSION+'_'+subject.VERSION
    assert version_for_symbol('AAPL')==VERSION


def test_finite_individual_integers_cannot_overflow_derived_result():
    value=records()
    for name in ('hca_labor','hca_supplies','hca_other','hca_da'):
        value[name][PERIOD]['value']=10**308
    actual,proof=apply(value)
    assert not actual['operating_income'] and proof['holds'][0]['reason']=='nonfinite_derivation'
