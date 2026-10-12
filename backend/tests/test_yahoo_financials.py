from copy import deepcopy
from datetime import datetime, timezone
import json
from urllib.parse import urlencode

import pytest

from app.clients.yahoo_financials import parse_statement, parse_timeseries, YahooStatementError

NOW = datetime(2026, 10, 12, tzinfo=timezone.utc)
PAGE = 'https://finance.yahoo.com/quote/MSFT/financials/'


def api_url(keys=('annualEBITDA', 'trailingEBITDA')):
    return 'https://query1.finance.yahoo.com/ws/fundamentals-timeseries/v1/finance/timeseries/MSFT?'+urlencode({
        'merge':'false','padTimeSeries':'true','period1':1700000000,'period2':1800000000,
        'type':','.join(keys),'lang':'en-US','region':'US'})


def point(value=123456789, end='2026-06-30', period='12M'):
    return {'asOfDate':end,'periodType':period,'currencyCode':'USD',
        'reportedValue':{'raw':value,'fmt':'WRONG DISPLAY VALUE'}}


def row(key='annualEBITDA', values=None):
    return {'meta':{'symbol':['MSFT'],'type':[key]},key:values if values is not None else [point()]}


def body(rows=None):
    return {'timeseries':{'result':rows if rows is not None else [row()], 'error':None}}


def parse(data, **kwargs):
    return parse_timeseries(json.dumps(data).encode(),symbol='MSFT',statement='income',
        source_url=kwargs.pop('source_url',api_url()),observed_at=kwargs.pop('observed_at',NOW),**kwargs)


def html(data, canonical=PAGE):
    wrapper=json.dumps({'status':200,'body':json.dumps(data)})
    return (f'<html><link rel="canonical" href="{canonical}"><script type="application/json" '
        f'data-sveltekit-fetched data-url="{api_url()}">{wrapper}</script></html>').encode()


def test_raw_units_periods_and_provenance_are_distinct_and_repeatable():
    data=body([row(),row('trailingEBITDA',[point(234567890,period='TTM')])]);before=deepcopy(data)
    result=parse(data)
    assert result==parse(data) and data==before
    assert [p['value'] for p in result['values']]==[123456789,234567890]
    assert [p['frequency'] for p in result['values']]==['annual','trailing']
    assert result['provider']=='yahoo_finance' and result['status']=='evidence_only'
    assert result['observed_at']==NOW.isoformat() and len(result['source_sha256'])==64
    assert 'period_start' not in result['values'][0] and 'filed_at' not in result['values'][0]


def test_same_values_from_page_and_direct_json():
    data=body()
    page=parse_statement(html(data),symbol='MSFT',statement='income',source_url=PAGE,observed_at=NOW)
    api=parse(data)
    assert page['values']==api['values']
    assert page['source_sha256']!=api['source_sha256']


@pytest.mark.parametrize('value',[0,-123,1.125])
def test_zero_negative_and_fractional_values_are_preserved(value):
    assert parse(body([row(values=[point(value)])]))['values'][0]['value']==value


@pytest.mark.parametrize('value',[True,'123',None,float('nan'),float('inf'),10**1000])
def test_invalid_numeric_values_rejected(value):
    with pytest.raises(YahooStatementError):parse(body([row(values=[point(value)])]))


@pytest.mark.parametrize('end,period',[('2026-02-30','12M'),('2027-06-30','12M'),('2026-06-30','TTM'),('06/30/2026','12M')])
def test_invalid_or_incompatible_period_rejected(end,period):
    with pytest.raises(YahooStatementError):parse(body([row(values=[point(end=end,period=period)])]))


@pytest.mark.parametrize('mutation',[
    lambda d:d['timeseries'].update(error={'description':'not available'}),
    lambda d:d['timeseries'].update(result=[]),
    lambda d:d['timeseries']['result'][0]['meta'].update(symbol=['NVDA']),
    lambda d:d['timeseries']['result'][0]['meta'].update(symbol=['MSFT','NVDA']),
    lambda d:d['timeseries']['result'][0]['meta'].update(type=['annualEBITDA','trailingEBITDA']),
    lambda d:d['timeseries']['result'][0]['annualEBITDA'][0].update(currencyCode=''),
    lambda d:d['timeseries']['result'][0]['annualEBITDA'][0].update(currencyCode='US dollars'),
    lambda d:d['timeseries']['result'][0]['annualEBITDA'][0].pop('reportedValue'),
])
def test_schema_and_identity_holds(mutation):
    data=body();mutation(data)
    with pytest.raises(YahooStatementError):parse(data)


def test_missing_is_not_zero_and_exact_duplicates_do_not_duplicate():
    result=parse(body([row(values=[None,point(),point()]),row('trailingEBITDA',[])]))
    assert len(result['values'])==1
    assert result['unavailable_series']==['trailingEBITDA']
    assert result['null_observation_counts']=={'annualEBITDA':1}
    with pytest.raises(YahooStatementError,match='conflicting_statement_point'):
        parse(body([row(values=[point(),point(456)])]))


def test_currency_conflict_is_not_combined():
    other=point();other['currencyCode']='CAD'
    with pytest.raises(YahooStatementError,match='conflicting_statement_point'):
        parse(body([row(values=[point(),other])]))


def test_normalized_and_reported_ebitda_never_alias():
    result=parse(body([row(),row('annualNormalizedEBITDA',[point(99)])]),
        source_url=api_url(('annualEBITDA','annualNormalizedEBITDA')))
    assert {p['metric']:p['value'] for p in result['values']}=={'EBITDA':123456789,'NormalizedEBITDA':99}


@pytest.mark.parametrize('url',[
    api_url().replace('query1.finance.yahoo.com','example.com'),
    api_url().replace('https://','http://'),
    api_url().replace('/MSFT?','/NVDA?'),
    api_url()+'&crumb=unreviewed',api_url()+'&type=annualEBITDA',
    api_url().replace('merge=false','merge=true'),api_url()+'#fragment',
])
def test_unreviewed_endpoint_identity_rejected(url):
    with pytest.raises(YahooStatementError):parse(body(),source_url=url)


def test_unrequested_metric_and_naive_time_rejected():
    with pytest.raises(YahooStatementError,match='unrequested_statement_metric'):
        parse(body([row('annualTotalRevenue')]))
    with pytest.raises(YahooStatementError,match='observation_time_required'):
        parse(body(),observed_at=datetime(2026,10,12))


@pytest.mark.parametrize('raw',[
    html(body(),canonical=PAGE.replace('MSFT','NVDA')),
    html(body()).replace(b'"status": 200',b'"status": 401'),
    html(body())+html(body()), b'<html>Consent or unavailable</html>',
])
def test_html_success_code_is_not_proof_of_financial_data(raw):
    with pytest.raises(YahooStatementError):
        parse_statement(raw,symbol='MSFT',statement='income',source_url=PAGE,observed_at=NOW)


def test_duplicate_json_key_rejected():
    raw=b'{"timeseries":{},"timeseries":{"result":[],"error":null}}'
    with pytest.raises(YahooStatementError):
        parse_timeseries(raw,symbol='MSFT',statement='income',source_url=api_url(),observed_at=NOW)


def test_supported_balance_and_cash_flow_raw_signs():
    for statement,metric,value in [('balance','NetDebt',-100),('cash_flow','CapitalExpenditure',-200)]:
        key='annual'+metric
        result=parse_timeseries(json.dumps(body([row(key,[point(value)])])).encode(),
            symbol='MSFT',statement=statement,source_url=api_url((key,)),observed_at=NOW)
        assert result['values'][0]['value']==value
        assert result['values'][0]['period_basis']==('instant' if statement=='balance' else 'duration')


def test_captured_msft_public_series_keep_actual_values_and_definitions():
    from pathlib import Path
    source=Path(__file__).parent/'fixtures/yahoo_msft_financials_2026_10_12.json'
    data=json.loads(source.read_text())
    keys=tuple(r['meta']['type'][0] for r in data['timeseries']['result'])
    result=parse(data,source_url=api_url(keys))
    latest={p['source_metric']:p['value'] for p in result['values'] if p['provider_as_of_date']=='2026-06-30'}
    assert latest=={'annualEBITDA':207519000000.0,'trailingEBITDA':207519000000.0,
        'annualNetIncome':133749000000.0,'annualReconciledDepreciation':38534000000.0}
    assert data['capture']['original_response_sha256']=='3e6ceb158f3d614ce40c601edad351be92ea359da7216253afe335d7d3960c2f'
    assert len(result['values'])==14


def test_captured_quarterly_data_does_not_become_annual_or_trailing():
    from pathlib import Path
    raw=(Path(__file__).parent/'fixtures/yahoo_msft_quarterly_2026_10_12.json').read_bytes()
    data=json.loads(raw)
    keys=tuple(r['meta']['type'][0] for r in data['timeseries']['result'])
    result=parse(data,source_url=api_url(keys))
    assert len(result['values'])==20
    assert {p['frequency'] for p in result['values']}=={'quarterly'}
    assert {p['provider_period_type'] for p in result['values']}=={'3M'}
    assert next(p['value'] for p in result['values'] if p['metric']=='TotalRevenue' and p['provider_as_of_date']=='2026-06-30')==90007000000
    data['timeseries']['result'][0][keys[0]][0]['periodType']='12M'
    with pytest.raises(YahooStatementError,match='statement_period_mismatch'):
        parse(data,source_url=api_url(keys))
