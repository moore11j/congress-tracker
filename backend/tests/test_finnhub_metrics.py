from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
import json

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from app.db import Base
from app.models import FundamentalsCache, InsightsSnapshot
from app.services import finnhub_metrics as m, free_fundamentals as f, fundamentals_cache as cache
from app.services.finnhub_research import FinnhubUnavailable

NOW = datetime(2026, 10, 10, tzinfo=timezone.utc)


def payload():
    return {'symbol': 'ABC', 'metricType': 'all',
        'metric': {'peTTM': 20, 'forwardPE': 16, 'psTTM': 5, 'evEbitdaTTM': 18,
            'grossMarginTTM': 0.5, 'operatingMarginTTM': 30, 'netProfitMarginTTM': -2,
            'revenueGrowthTTMYoy': 12, 'epsGrowthTTMYoy': 7, 'marketCapitalization': 1000,
            'currentRatioQuarterly': 999, 'epsTTM': 100},
        'series': {'quarterly': {k: [{'period': '2026-06-30', 'v': v}] for k,v in
            {'currentRatio': 2, 'totalDebtToEquity': -1, 'roeTTM': .25, 'roicTTM': .17,
             'operatingMargin': .99}.items()}}}


def sec():
    return {'symbol': 'ABC', 'provider': 'sec_edgar', 'company_name': 'ABC company',
        'fetched_at': NOW, 'period_date': date(2026,6,30), 'status': 'ok',
        'revenue_growth': 15, 'roe': 21, 'free_cash_flow': 500,
        'source_evidence_json': json.dumps({'methodology': 'sec_fundamentals_v1',
            'current': {'revenue': {'value': 1000}}, 'market': {'provider':'massive'}})}


def test_units_periods_and_unavailable_fields_are_not_guessed():
    result = m.normalize(payload(), symbol='ABC', observed_at=NOW)
    values = result['values']
    assert values['roe'] == 25 and values['roic'] == 17
    assert values['gross_margin'] == .5 and values['operating_margin'] == 30
    assert values['current_ratio'] == 2 and values['debt_to_equity'] == -1
    assert values['forward_pe'] == 16
    assert result['fields']['ev_to_ebitda']['period_date'] is None
    assert result['fields']['roe']['period_date'] == '2026-06-30'
    assert not {'price', 'eps_ttm', 'market_cap', 'net_debt_to_ebitda', 'free_cash_flow'} & values.keys()
    assert result == m.normalize(payload(), symbol='ABC', observed_at=NOW)


@pytest.mark.parametrize('returned', ['BRK.A', '2330.TW', '', None])
def test_wrong_share_or_adr_is_rejected(returned):
    raw = payload(); raw['symbol'] = returned
    with pytest.raises(FinnhubUnavailable, match='symbol_mismatch'):
        m.normalize(raw, symbol='ABC', observed_at=NOW)


@pytest.mark.parametrize('value', [True, '20', float('nan'), float('inf'), 1e308])
def test_invalid_numbers_never_enter_cache(value):
    raw=payload(); raw['series']['quarterly']['roeTTM'][0]['v']=value
    with pytest.raises(FinnhubUnavailable):
        m.normalize(raw,symbol='ABC',observed_at=NOW)


def test_out_of_order_duplicates_conflicts_future_and_stale():
    raw=payload()
    raw['series']['quarterly']['currentRatio'] += [{'period':'2026-03-31','v':8},
        {'period':'2026-06-30','v':2}]
    assert m.normalize(raw,symbol='ABC',observed_at=NOW)['values']['current_ratio']==2
    raw['series']['quarterly']['currentRatio'][-1]['v']=3
    with pytest.raises(FinnhubUnavailable,match='conflicting'):
        m.normalize(raw,symbol='ABC',observed_at=NOW)
    raw=payload(); raw['series']['quarterly']['roeTTM'][0]['period']='2026-12-31'
    with pytest.raises(FinnhubUnavailable,match='future'):
        m.normalize(raw,symbol='ABC',observed_at=NOW)
    assert m.normalize(payload(),symbol='ABC',observed_at=NOW+timedelta(days=100))['values']=={}
    raw=payload(); raw['series']={}
    assert m.normalize(raw,symbol='ABC',observed_at=NOW)['values']=={}


def test_null_latest_does_not_resurrect_older_ratio():
    raw=payload(); raw['series']['quarterly']['roeTTM'] += [{'period':'2026-07-31','v':None}]
    assert 'roe' not in m.normalize(raw,symbol='ABC',observed_at=NOW)['values']


def test_combination_preserves_sec_and_massive_evidence_with_explicit_metrics():
    original=sec(); result=f.combine(original,m.normalize(payload(),symbol='ABC',observed_at=NOW))
    assert original==sec() and result['provider']=='free_direct'
    assert result['roe']==21 and result['revenue_growth']==15 and result['free_cash_flow']==500
    assert result['ev_to_ebitda']==18
    evidence=json.loads(result['source_evidence_json'])
    assert evidence['market']=={'provider':'massive'} and 'roe' not in evidence['supplement']['used_fields']
    assert evidence['supplement']['fields']['ev_to_ebitda']['period_date'] is None
    newer=sec(); newer['period_date']=date(2026,9,30)
    assert 'current_ratio' not in f.combine(newer,m.normalize(payload(),symbol='ABC',observed_at=NOW))


@pytest.fixture
def factory(monkeypatch):
    engine=create_engine('sqlite:///:memory:'); Base.metadata.create_all(engine)
    sessions=sessionmaker(bind=engine); monkeypatch.setattr(m,'SessionLocal',sessions)
    monkeypatch.setenv('FINNHUB_METRICS_WARMING_ENABLED','1')
    monkeypatch.setenv('FINNHUB_SHARED_LIMITER_ENABLED','1')
    yield sessions
    engine.dispose()


def test_cache_repeat_transport_outside_transaction_failure_retains_prior(factory,monkeypatch):
    calls=[]
    def fetch(*a):
        calls.append(a)
        raw=payload()
        for rows in raw['series']['quarterly'].values():
            rows[0]['period']=(datetime.now(timezone.utc).date()-timedelta(days=40)).isoformat()
        return raw
    monkeypatch.setattr(m,'request_json',fetch)
    first=m.prepare('ABC'); assert m.prepare('ABC')==first and len(calls)==1
    with factory() as db:
        row=db.get(InsightsSnapshot,'finnhub:metrics:ABC'); row.fetched_at-=timedelta(days=2)
        db.commit(); saved=row.payload_json
    def deny(*a): raise FinnhubUnavailable('rate_limited')
    monkeypatch.setattr(m,'request_json',deny)
    with pytest.raises(FinnhubUnavailable,match='rate_limited'): m.prepare('ABC')
    with factory() as db: assert db.get(InsightsSnapshot,'finnhub:metrics:ABC').payload_json==saved


def test_selected_consumer_uses_separate_cache_and_never_fmp(factory,monkeypatch):
    monkeypatch.setenv('FUNDAMENTALS_PROVIDER','free_direct')
    monkeypatch.setattr(f.sec_fundamentals,'fetch_fundamentals',lambda s:sec())
    monkeypatch.setattr(m,'prepare',lambda s:m.normalize(payload(),symbol=s,observed_at=NOW))
    monkeypatch.setattr(cache,'_request_rows',lambda *a,**k:pytest.fail('FMP request'))
    monkeypatch.setattr(cache,'fetch_company_profile',lambda *a,**k:pytest.fail('FMP profile'))
    result=cache.fetch_fundamentals_for_symbol('ABC'); assert result.status=='ok'
    with factory() as db:
        db.add(FundamentalsCache(symbol='ABC',provider='fmp',roe=99,fetched_at=NOW,status='ok'))
        assert cache.upsert_fundamentals_cache(db,result.values); db.commit()
        rows={r.provider:r for r in db.scalars(select(FundamentalsCache))}
        assert rows['fmp'].roe==99
        summary=cache.fundamentals_summary_from_cache_row(rows['free_direct'],now=NOW)
        assert summary['metrics']['return_on_equity']['value']==.21
        assert summary['metrics']['ev_to_ebitda']['provenance']['period_date'] is None
        assert summary['context']['source_label']=='SEC EDGAR + Finnhub'
        assert cache.cache_row_to_screener_row(rows['free_direct'])['ev_ebitda']==18
        before=rows['free_direct'].updated_at
        cache.upsert_fundamentals_cache(db,result.values); db.commit()
        assert rows['free_direct'].updated_at==before


def test_source_failure_keeps_sec_without_fmp_or_false_supplement(monkeypatch):
    monkeypatch.setattr(f.sec_fundamentals,'fetch_fundamentals',lambda s:sec())
    def deny(*a): raise FinnhubUnavailable('metric_symbol_mismatch')
    monkeypatch.setattr(m,'prepare',deny)
    result=f.fetch_fundamentals('ABC')
    assert result['roe']==21 and 'ev_to_ebitda' not in result
    assert json.loads(result['source_evidence_json'])['supplement_error']=='metric_symbol_mismatch'
