from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
import json
from urllib.parse import urlencode

import pytest

from app.clients.yahoo_financials import YahooStatementError
from app.services.yahoo_financial_evidence import prepare_bundle

NOW = datetime(2026, 10, 12, tzinfo=timezone.utc)


def capture(rows, statement='income', symbol='MSFT'):
    keys = [r['meta']['type'][0] for r in rows]
    url = f'https://query1.finance.yahoo.com/ws/fundamentals-timeseries/v1/finance/timeseries/{symbol}?'+urlencode({
        'merge':'false', 'padTimeSeries':'true', 'period1':1700000000, 'period2':1800000000,
        'type':','.join(keys), 'lang':'en-US', 'region':'US'})
    return {'raw':json.dumps({'timeseries':{'result':rows,'error':None}}).encode(),
        'statement':statement,'source_url':url,'observed_at':NOW}


def row(key, values, currency='USD'):
    return {'meta':{'symbol':['MSFT'],'type':[key]}, key:[{
        'asOfDate':end, 'periodType':'3M' if key.startswith('quarterly') else '12M',
        'currencyCode':currency, 'reportedValue':{'raw':value}} for end,value in values]}


def example(metric='EBITDA'):
    return capture([row('annual'+metric,[('2026-06-30',10)]),
        row('quarterly'+metric,[('2025-09-30',1),('2025-12-31',2),('2026-03-31',3),('2026-06-30',4)])])


def test_actual_source_conflicts_are_explicit_and_not_selected():
    sources=[]
    for name in ('yahoo_msft_financials_2026_10_12.json','yahoo_msft_quarterly_2026_10_12.json'):
        data=json.loads((Path(__file__).parent/'fixtures'/name).read_bytes())
        sources.append(capture(data['timeseries']['result']))
    original=deepcopy(sources)
    result=prepare_bundle(sources,symbol='MSFT')
    current={r['metric']:r for r in result['comparisons'] if r['frequency']=='annual' and r['provider_as_of_date']=='2026-06-30'}
    for metric in ('EBITDA','ReconciledDepreciation'):
        assert current[metric]['status']=='differs'
        assert float(current[metric]['difference'])==4914000000
        assert not current[metric]['canonical_eligible']
    assert current['NetIncome']['status']=='incomplete_quarter_window'
    assert not result['canonical_eligible'] and not result['conflicts']
    assert result==prepare_bundle(sources,symbol='MSFT') and sources==original
    assert result==prepare_bundle(list(reversed(sources)),symbol='MSFT')


def test_matching_sum_remains_diagnostic_only_and_repeated_captures_deduplicate():
    source=example()
    result=prepare_bundle([source,source],symbol='MSFT')
    assert len(result['sources'])==1 and len(result['observations'])==5
    assert result['comparisons'][0]['status']=='agrees'
    assert result['comparisons'][0]['candidate_sum']=='10'
    assert not result['canonical_eligible']


@pytest.mark.parametrize('kind',['value','currency'])
def test_conflicting_revisions_hold_point_and_do_not_pick_latest(kind):
    source=example(); changed=deepcopy(source)
    data=json.loads(changed['raw']);point=data['timeseries']['result'][1]['quarterlyEBITDA'][0]
    if kind=='value':point['reportedValue']['raw']=100
    else:point['currencyCode']='CAD'
    changed['raw']=json.dumps(data).encode();changed['observed_at']=NOW.replace(hour=1)
    result=prepare_bundle([source,changed],symbol='MSFT')
    assert len(result['conflicts'])==1
    assert result['comparisons'][0]['status']=='conflicting_quarter_observations'
    assert 'candidate_sum' not in result['comparisons'][0]
    assert result==prepare_bundle([changed,source],symbol='MSFT')


def test_currency_mismatch_prevents_sum():
    source=example();data=json.loads(source['raw'])
    for point in data['timeseries']['result'][1]['quarterlyEBITDA']:point['currencyCode']='CAD'
    source['raw']=json.dumps(data).encode()
    result=prepare_bundle([source],symbol='MSFT')
    assert result['comparisons'][0]['status']=='currency_mismatch'
    assert 'candidate_sum' not in result['comparisons'][0]


@pytest.mark.parametrize('mutation', ['missing','gap','end'])
def test_incomplete_or_nonconsecutive_window_is_not_summed(mutation):
    source=example();data=json.loads(source['raw']);points=data['timeseries']['result'][1]['quarterlyEBITDA']
    if mutation=='missing':points.pop(1)
    elif mutation=='gap':points[0]['asOfDate']='2025-05-31'
    else:points[-1]['asOfDate']='2026-05-31'
    source['raw']=json.dumps(data).encode()
    result=prepare_bundle([source],symbol='MSFT')
    assert result['comparisons'][0]['status']=='incomplete_quarter_window'
    assert 'candidate_sum' not in result['comparisons'][0]


def test_balance_sheet_never_sums_quarters():
    source=example('TotalDebt');source['statement']='balance'
    assert prepare_bundle([source],symbol='MSFT')['comparisons']==[]


def test_decimal_comparison_does_not_create_binary_rounding_conflict():
    source=capture([row('annualEBITDA',[('2026-06-30',0.3)]),row('quarterlyEBITDA',[
        ('2025-09-30',0.1),('2025-12-31',0.2),('2026-03-31',0),('2026-06-30',0)])])
    assert prepare_bundle([source],symbol='MSFT')['comparisons'][0]['status']=='agrees'


def test_reported_and_normalized_ebitda_do_not_reconcile_with_each_other():
    source=example();data=json.loads(source['raw']);other=data['timeseries']['result'][1]
    other['quarterlyNormalizedEBITDA']=other.pop('quarterlyEBITDA');other['meta']['type']=['quarterlyNormalizedEBITDA']
    result=prepare_bundle([capture(data['timeseries']['result'])],symbol='MSFT')
    assert result['comparisons'][0]['status']=='incomplete_quarter_window'


@pytest.mark.parametrize('sources',[[],[{}],[example()]*7,[dict(example(),raw=b'x'*4_000_001)]])
def test_bundle_resource_bounds(sources):
    with pytest.raises(YahooStatementError):prepare_bundle(sources,symbol='MSFT')


def test_other_symbol_rejected_before_values_can_be_combined():
    with pytest.raises(YahooStatementError):prepare_bundle([example()],symbol='NVDA')
