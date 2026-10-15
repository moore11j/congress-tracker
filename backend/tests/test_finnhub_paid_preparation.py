from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from app.models import InsightsSnapshot
from app.services import finnhub_paid_preparation as prep, finnhub_research as transport

NOW = datetime(2026, 10, 15, 12, tzinfo=timezone.utc)


def estimate(kind='eps', **changes):
    return {'period': '2026-12-31', 'year': 2027, 'quarter': 1, 'numberAnalysts': 12,
            kind+'Low': -2, kind+'Avg': 0, kind+'High': 3, **changes}


def response(kind='eps', rows=None, **changes):
    return {'symbol': 'AAPL', 'freq': 'quarterly', 'data': rows if rows is not None else [estimate(kind)], **changes}


def target(**changes):
    return {'symbol': 'AAPL', 'lastUpdated': '2026-10-14', 'numberAnalysts': 8,
            'targetLow': 100, 'targetMean': 150, 'targetMedian': 145, 'targetHigh': 200, **changes}


def normalize(raw, kind='eps', **changes):
    args = {'frequency': None if kind == 'targets' else 'quarterly', 'observed_at': NOW, **changes}
    return prep.normalize(raw, 'AAPL', kind, **args)


def forbidden(*a, **kw):
    pytest.fail('Unexpected provider/database call')


def test_preserves_fiscal_identity_negative_eps_zero_and_proprietary_basis():
    raw = response(rows=[estimate(), estimate()])
    saved = deepcopy(raw)
    result = normalize(raw)
    assert len(result['items']) == 1
    assert result['nextPeriod']['average'] == 0
    assert result['nextPeriod']['fiscalYear'] == 2027
    assert result['nextPeriod']['low'] == -2
    assert result['estimateBasis'] == 'provider_average_includes_proprietary_estimates'
    assert result['currency'] is None and not result['publicationEligible']
    assert result['accountingBasis'] == result['shareBasis'] == 'unverified'
    assert result['sourceUpdatedAt'] is None
    assert raw == saved and normalize(raw) == result


@pytest.mark.parametrize('kind', ['eps', 'revenue'])
def test_annual_and_quarterly_have_distinct_fiscal_rows_and_keep_native_scale(kind):
    raw = response(kind, rows=[estimate(kind, **{kind+'Low': 1e9, kind+'Avg': 2e9, kind+'High': 3e9, 'quarter': 0})], freq='annual')
    result = normalize(raw, kind, frequency='annual')
    assert result['nextPeriod']['average'] == 2e9
    assert result['frequency'] == 'annual'
    assert result['estimateBasis'] == 'provider_average_includes_proprietary_estimates'


@pytest.mark.parametrize('changes', [{'symbol': 'MSFT'}, {'symbol': 'BRK.A'}, {'freq': 'annual'}, {'data': {}}, {'error': 'denied'}])
def test_wrong_identity_frequency_or_error_is_held(changes):
    with pytest.raises(transport.FinnhubUnavailable):
        normalize(response(**changes))


@pytest.mark.parametrize('changes', [
    {'epsAvg': True}, {'epsAvg': float('nan')}, {'epsAvg': float('inf')}, {'epsAvg': '2.3'},
    {'epsAvg': 7}, {'epsLow': 4}, {'numberAnalysts': -1}, {'numberAnalysts': True},
    {'numberAnalysts': 1.1}, {'year': None}, {'year': True}, {'quarter': 0},
    {'quarter': 5}, {'period': '2026-02-30'}, {'period': '20261231'},
])
def test_bad_estimate_rows_are_held(changes):
    with pytest.raises(transport.FinnhubUnavailable):
        normalize(response(rows=[estimate(**changes)]))


def test_missing_values_stay_missing_and_no_analysts_is_partial():
    result = normalize(response(rows=[estimate(epsAvg=None, numberAnalysts=0)]))
    assert result['status'] == 'partial' and result['nextPeriod']['average'] is None


def test_bad_revenue_range_and_annual_quarter_are_held():
    with pytest.raises(transport.FinnhubUnavailable, match='range'):
        normalize(response('revenue'), 'revenue')
    with pytest.raises(transport.FinnhubUnavailable, match='annual_quarter'):
        normalize(response(freq='annual'), frequency='annual')


@pytest.mark.parametrize('second', [estimate(epsAvg=1), estimate(year=2026), estimate(period='2027-01-01')])
def test_conflicting_duplicate_period_or_fiscal_identity_is_held(second):
    with pytest.raises(transport.FinnhubUnavailable, match='conflicting'):
        normalize(response(rows=[estimate(), second]))


def test_complete_nested_array_limit_is_enforced_not_top_level_only():
    with pytest.raises(transport.FinnhubUnavailable, match='response_too_large'):
        normalize(response(rows=[estimate()] * 1001))


def test_historical_only_does_not_become_next_forecast_and_empty_is_explicit():
    result = normalize(response(rows=[estimate(period='2026-09-30')]))
    assert result['nextPeriod'] is None and result['status'] == 'historical_only'
    assert len(result['items']) == 1
    assert normalize(response(rows=[]))['status'] == 'empty'


def test_price_targets_preserve_update_time_and_never_calculate_upside():
    result = normalize(target(), 'targets')
    assert result['targetMean'] == 150 and result['targetMedian'] == 145
    assert result['sourceUpdatedAt'] == '2026-10-14'
    assert result['targetHorizon'] == 'unverified'
    assert result['status'] == 'prepared' and not result['publicationEligible']
    assert 'upside' not in result and result['currency'] is None
    assert normalize(target(lastUpdated='2026-08-01'), 'targets')['status'] == 'stale'


@pytest.mark.parametrize('changes', [{'lastUpdated': '2026-10-16'}, {'lastUpdated': None},
    {'lastUpdated': '2026-10-14T12:00:00'}, {'lastUpdated': '2026-10-15T13:00:00Z'},
    {'targetMean': 999}, {'targetMedian': 5}, {'targetLow': -1}, {'targetHigh': False},
    {'numberAnalysts': -1}, {'targetMean': '150'}])
def test_invalid_target_values_and_dates_are_held(changes):
    with pytest.raises(transport.FinnhubUnavailable):
        normalize(target(**changes), 'targets')


def test_aware_target_timestamp_is_preserved():
    stamp = '2026-10-15T10:00:00+00:00'
    assert normalize(target(lastUpdated=stamp), 'targets')['sourceUpdatedAt'] == stamp


@pytest.mark.parametrize('symbol,kind,freq', [('NYSE:AAPL','eps','annual'), ('AAPL?token=x','eps','annual'),
    ('AAPL','eps',None), ('AAPL','targets','annual'), ('AAPL','prices',None)])
def test_invalid_scope_cannot_request(symbol, kind, freq):
    with pytest.raises(transport.FinnhubUnavailable):
        prep.scope(symbol, kind, freq)


def test_share_class_punctuation_retains_class():
    assert prep.scope('BRK-B','eps','annual')[1] == 'BRK.B'
    result = prep.normalize(response(symbol='BRK.B'), 'BRK-B','eps',frequency='quarterly',observed_at=NOW)
    assert result['symbol'] == 'BRK-B'
    with pytest.raises(transport.FinnhubUnavailable):
        prep.normalize(response(symbol='BRK.A'), 'BRK-B','eps',frequency='quarterly',observed_at=NOW)


@pytest.fixture
def db(monkeypatch):
    monkeypatch.setenv('FINNHUB_PAID_PREPARATION_ENABLED','1')
    monkeypatch.setenv('FINNHUB_SHARED_LIMITER_ENABLED','1')
    engine = create_engine('sqlite:///:memory:')
    InsightsSnapshot.__table__.create(engine)
    with sessionmaker(bind=engine)() as session:
        yield session
    engine.dispose()


def test_disabled_checks_precede_db_and_network(monkeypatch):
    monkeypatch.delenv('FINNHUB_PAID_PREPARATION_ENABLED', raising=False)
    monkeypatch.setattr(transport.requests,'get',forbidden)
    with pytest.raises(transport.FinnhubUnavailable,match='paid_preparation_disabled'):
        prep.prepare(None,'AAPL','eps',frequency='annual')
    with pytest.raises(transport.FinnhubUnavailable,match='paid_preparation_disabled'):
        transport.request_json('stock/price-target', {'symbol':'AAPL'})
    monkeypatch.setenv('FINNHUB_PAID_PREPARATION_ENABLED','1')
    monkeypatch.delenv('FINNHUB_SHARED_LIMITER_ENABLED',raising=False)
    with pytest.raises(transport.FinnhubUnavailable,match='shared_request_budget_required'):
        prep.prepare(None,'AAPL','targets')


def test_all_five_scopes_persist_repeat_without_requests_or_canonical_tables(db,monkeypatch):
    calls=[]
    def fetch(path,params):
        calls.append((path,params))
        if path.endswith('price-target'):
            return target()
        kind = 'eps' if path.endswith('eps-estimate') else 'revenue'
        return response(kind,rows=[estimate(kind, **{kind+'Low':1,kind+'Avg':2,kind+'High':3,
            'quarter':0 if params['freq']=='annual' else 1})],freq=params['freq'])
    monkeypatch.setattr(transport,'request_json',fetch)
    scopes=[('eps','annual'),('eps','quarterly'),('revenue','annual'),('revenue','quarterly'),('targets',None)]
    for kind,freq in scopes:
        assert prep.prepare(db,'AAPL',kind,frequency=freq,observed_at=NOW)['status']=='prepared'
    db.commit()
    before=[(r.kind,r.payload_json,r.fetched_at) for r in db.scalars(select(InsightsSnapshot)).all()]
    for kind,freq in scopes:
        assert prep.prepare(db,'AAPL',kind,frequency=freq,observed_at=NOW+timedelta(hours=1))['status']=='cached'
    after=[(r.kind,r.payload_json,r.fetched_at) for r in db.scalars(select(InsightsSnapshot)).all()]
    assert before == after and len(calls)==5 and len(before)==5
    for _,body,_ in after:
        saved=json.loads(body)
        assert saved['normalized']['sourceSha256']==prep._digest(saved['raw'])
        assert not saved['normalized']['publicationEligible']


def test_expired_cache_refresh_failure_preserves_original_evidence(db,monkeypatch):
    monkeypatch.setattr(transport,'request_json',lambda *a: response())
    prep.prepare(db,'AAPL','eps',frequency='quarterly',observed_at=NOW)
    db.commit()
    row=db.scalar(select(InsightsSnapshot)); before=(row.payload_json,row.fetched_at)
    def denied(*a):
        raise transport.FinnhubUnavailable('access_denied')
    monkeypatch.setattr(transport,'request_json',denied)
    with pytest.raises(transport.FinnhubUnavailable,match='access_denied'):
        prep.prepare(db,'AAPL','eps',frequency='quarterly',observed_at=NOW+timedelta(days=1))
    assert (row.payload_json,row.fetched_at)==before


def test_corrupted_saved_projection_must_not_pass_cache_validation(db,monkeypatch):
    calls=[]
    monkeypatch.setattr(transport,'request_json',lambda *a: calls.append(a) or response())
    prep.prepare(db,'AAPL','eps',frequency='quarterly',observed_at=NOW)
    row=db.scalar(select(InsightsSnapshot)); body=json.loads(row.payload_json)
    body['normalized']['items'][0]['average']=999
    row.payload_json=json.dumps(body);db.commit()
    assert prep.prepare(db,'AAPL','eps',frequency='quarterly',observed_at=NOW)['status']=='prepared'
    assert len(calls)==2
    assert json.loads(row.payload_json)['normalized']['items'][0]['average']==0


def test_cli_disabled_does_not_open_database(monkeypatch,capsys):
    from app.jobs.prepare_finnhub_paid import main
    monkeypatch.delenv('FINNHUB_PAID_PREPARATION_ENABLED',raising=False)
    from app import db as database
    monkeypatch.setattr(database,'SessionLocal',forbidden)
    assert main(['--symbol','AAPL','--kind','targets'])==1
    assert json.loads(capsys.readouterr().out)['reason']=='paid_preparation_disabled'


def test_midnight_cache_status_rechecked_without_advancing_observation(db,monkeypatch):
    start=NOW.replace(hour=23)
    monkeypatch.setattr(transport,'request_json',lambda *a: response(rows=[estimate(period='2026-10-15')]))
    prep.prepare(db,'AAPL','eps',frequency='quarterly',observed_at=start)
    db.commit()
    row=db.scalar(select(InsightsSnapshot));before=row.payload_json
    monkeypatch.setattr(transport,'request_json',forbidden)
    result=prep.prepare(db,'AAPL','eps',frequency='quarterly',observed_at=start+timedelta(hours=2))
    assert result['status']=='cached' and result['sourceStatus']=='historical_only'
    assert row.payload_json==before


@pytest.mark.parametrize('code,reason', [(401,'authentication_failed'), (403,'access_denied'),
    (429,'rate_limited'),(302,'provider_unavailable')])
def test_paid_transport_shared_budget_header_auth_no_retry(db,monkeypatch,code,reason):
    from app.services import finnhub_budget as budget
    calls=[];reserved=[];cooled=[]
    monkeypatch.setenv('FINNHUB_API_KEY','synthetic-test-key')
    monkeypatch.setattr(budget,'reserve',lambda:reserved.append(True))
    monkeypatch.setattr(budget,'cooldown',lambda value:cooled.append(value))
    class Response:
        status_code=code
        headers={'Retry-After':'120'}
        def __enter__(self):return self
        def __exit__(self,*args):pass
    def request(url,**kwargs):
        calls.append((url,kwargs));return Response()
    monkeypatch.setattr(transport.requests,'get',request)
    with pytest.raises(transport.FinnhubUnavailable,match=reason) as error:
        prep.prepare(db,'AAPL','targets',observed_at=NOW)
    assert len(calls)==len(reserved)==1
    url,kwargs=calls[0]
    assert url=='https://finnhub.io/api/v1/stock/price-target'
    assert kwargs['headers']=={'X-Finnhub-Token':'synthetic-test-key'}
    assert kwargs['params']=={'symbol':'AAPL'} and kwargs['allow_redirects'] is False
    assert 'synthetic-test-key' not in str(error.value)
    assert cooled==(['120'] if code==429 else [])
    assert db.scalar(select(InsightsSnapshot)) is None


def test_enabled_paid_transport_cannot_bypass_exhausted_shared_budget(db,monkeypatch):
    from app.services import finnhub_budget as budget
    monkeypatch.setenv('FINNHUB_API_KEY','synthetic-test-key')
    monkeypatch.setattr(transport.requests,'get',forbidden)
    def exhausted():raise transport.FinnhubUnavailable('request_budget_exhausted')
    monkeypatch.setattr(budget,'reserve',exhausted)
    with pytest.raises(transport.FinnhubUnavailable,match='request_budget_exhausted'):
        prep.prepare(db,'AAPL','targets',observed_at=NOW)


def test_paid_staging_cannot_leak_into_selected_analyst_reader(db,monkeypatch):
    from app.services import replacement_analysts
    now=datetime.now(timezone.utc)
    raw=[{'symbol':'AAPL','period':now.date().replace(day=1).isoformat(),
          'strongBuy':2,'buy':3,'hold':4,'sell':1,'strongSell':0}]
    normalized=transport.normalize_recommendations(raw,'AAPL',observed_at=now)
    db.add(InsightsSnapshot(kind='finnhub:recommendations:AAPL',source='finnhub',
        fetched_at=now,payload_json=json.dumps(normalized)))
    db.commit()
    before=replacement_analysts.current_payload(db,'AAPL',include_details=True)
    monkeypatch.setattr(transport,'request_json',lambda *a:target())
    prep.prepare(db,'AAPL','targets',observed_at=NOW)
    db.commit()
    monkeypatch.setattr(transport.requests,'get',forbidden)
    after=replacement_analysts.current_payload(db,'AAPL',include_details=True)
    assert before==after
    assert after['currentSummary']['consensusImpliedUpsidePct'] is None


def test_successful_transport_persists_complete_provider_response(db,monkeypatch):
    from app.services import finnhub_budget as budget
    raw=response('revenue',rows=[estimate('revenue',revenueLow=100,revenueAvg=200,revenueHigh=300)])
    encoded=json.dumps(raw).encode()
    calls=[]
    class Response:
        status_code=200
        def __enter__(self):return self
        def __exit__(self,*args):pass
        def iter_content(self,size):
            yield encoded[:12];yield encoded[12:]
    monkeypatch.setenv('FINNHUB_API_KEY','synthetic-test-key')
    monkeypatch.setattr(budget,'reserve',lambda:None)
    monkeypatch.setattr(transport.requests,'get',lambda url,**kwargs:calls.append((url,kwargs)) or Response())
    result=prep.prepare(db,'AAPL','revenue',frequency='quarterly',observed_at=NOW)
    saved=json.loads(db.scalar(select(InsightsSnapshot)).payload_json)
    assert saved['raw']==raw and result['sourceStatus']=='prepared'
    assert saved['normalized']['nextPeriod']['average']==200
    assert len(calls)==1 and calls[0][1]['params']=={'symbol':'AAPL','freq':'quarterly'}
