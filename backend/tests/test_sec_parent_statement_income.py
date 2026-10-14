from copy import deepcopy
from datetime import date
import math
import pytest
from app.services import sec_parent_statement_income as review


def fixture(symbol):
    profile = review.reviews()[symbol]
    records = {'net_income': {}}
    for proof in profile['periods']:
        key = (date.fromisoformat(proof['start']), date.fromisoformat(proof['end']))
        for part in proof['inputs']:
            records.setdefault(part['metric'], {})[key] = {
                **{k:proof[k] for k in ('start','end','filed','accession','unit')},
                'source_cik':profile['cik'], 'tag':part['tag'], 'value':part['value'],
                'source_sha256':proof['source_sha256'], 'source_url':proof['url'], 'context_id':part['context']}
        for metric, guard in proof['allocation_guards'].items():
            if guard is not None and key not in records.get(metric, {}):
                records.setdefault(metric, {})[key] = deepcopy(guard)
    return records, profile


@pytest.mark.parametrize('symbol', ['FLEX','FOX','JEF'])
def test_reviewed_parent_income_with_exact_filing_identity_and_repeat(symbol):
    records, profile = fixture(symbol); before = deepcopy(records)
    output, evidence = review.apply(records, symbol=symbol, cik=profile['cik'])
    assert records == before
    assert len(evidence['accepted_periods']) == len(profile['periods'])
    for proof in profile['periods']:
        key = (date.fromisoformat(proof['start']),date.fromisoformat(proof['end']))
        result = output['net_income'][key]
        assert result['value'] == proof['value']
        assert result['value'] == sum(x['coefficient']*x['value'] for x in proof['statement_outputs'])
        assert result['parent_statement_review']['source_sha256'] == proof['source_sha256']
        assert review.is_reviewed({'inputs':[result]})
    again, _ = review.apply(output, symbol=symbol, cik=profile['cik'])
    assert again == output


@pytest.mark.parametrize('symbol', ['FLEX','FOX','JEF'])
@pytest.mark.parametrize('defect', ['missing','conflict','value','nan','bool','date','accession','currency','cik','hash','url','context','derived','tag','start'])
def test_no_substitution_for_changed_missing_or_unreviewed_sources(symbol,defect):
    records, profile = fixture(symbol); proof = profile['periods'][0]
    key = (date.fromisoformat(proof['start']), date.fromisoformat(proof['end']))
    series = records[proof['inputs'][0]['metric']]; fact = series[key]
    if defect == 'missing': del series[key]
    elif defect == 'conflict': series[key] = None
    elif defect == 'value': fact['value'] += 1
    elif defect == 'nan': fact['value'] = math.nan
    elif defect == 'bool': fact['value'] = True
    elif defect == 'date': fact['filed'] = '2026-10-14'
    elif defect == 'accession': fact['accession'] = 'changed'
    elif defect == 'currency': fact['unit'] = 'EUR'
    elif defect == 'cik': fact['source_cik'] = '0000000001'
    elif defect == 'hash': fact['source_sha256'] = 'changed'
    elif defect == 'url': fact['source_url'] = 'changed'
    elif defect == 'context': fact['context_id'] = 'changed'
    elif defect == 'derived': fact['derived'] = True
    elif defect == 'tag': fact['tag'] = 'IncomeLossAttributableToParent'
    elif defect == 'start': fact['start'] = '2020-01-01'
    output, evidence = review.apply(records, symbol=symbol, cik=profile['cik'])
    assert key not in output['net_income'] and evidence['holds']


@pytest.mark.parametrize('reported', [None, {'value': 123, 'tag':'NetIncomeLoss'}])
def test_explicit_parent_or_conflict_takes_priority(reported):
    records, profile = fixture('FOX'); key = next(iter(records['parent_income_consolidated']))
    records['net_income'][key] = reported
    output, _ = review.apply(records, symbol='FOX', cik=profile['cik'])
    assert output['net_income'][key] == reported


def test_unreviewed_company_cik_and_future_period_stay_unavailable():
    records, profile = fixture('FLEX')
    assert review.apply(records, symbol='OTHER', cik=profile['cik']) == (records,None)
    with pytest.raises(ValueError,match='cik_mismatch'):
        review.apply(records, symbol='FLEX', cik='0000000001')
    future = (date(2026,7,1),date(2026,9,30))
    records['parent_income_consolidated'] = {future:{'value':285000000}}
    output, _ = review.apply(records, symbol='FLEX', cik=profile['cik'])
    assert not output['net_income']


def test_only_reviewed_symbols_change_preparation_version():
    from app.services.sec_fundamentals_preparation import version_for_symbol, VERSION
    assert all(version_for_symbol(s) == VERSION+'_'+review.VERSION for s in review.SYMBOLS)
    assert version_for_symbol('AAPL') == VERSION


@pytest.mark.parametrize('symbol', ['FLEX','FOX','JEF'])
def test_new_or_changed_allocation_invalidates_review(symbol):
    records, profile = fixture(symbol); proof = profile['periods'][0]
    key = (date.fromisoformat(proof['start']),date.fromisoformat(proof['end']))
    metric = 'parent_income_redeemable'
    records.setdefault(metric,{})[key] = {'value': 999999}
    result, evidence = review.apply(records, symbol=symbol, cik=profile['cik'])
    assert key not in result['net_income']
    assert evidence['holds']['changed_allocation_evidence']
