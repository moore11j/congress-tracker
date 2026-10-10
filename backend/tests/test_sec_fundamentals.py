from datetime import datetime, timezone
import json
from copy import deepcopy

import pytest

from app.services.sec_fundamentals import project_fundamentals, SecFundamentalsError

NOW = datetime(2026, 10, 8, 18, tzinfo=timezone.utc)


def sources():
    periods = [('2024-01-01', '2024-12-31', '2025-02-01', 100),
               ('2025-01-01', '2025-12-31', '2026-02-01', 120),
               ('2024-01-01', '2024-06-30', '2024-08-01', 50),
               ('2025-01-01', '2025-06-30', '2025-08-01', 60),
               ('2026-01-01', '2026-06-30', '2026-08-01', 70)]
    gaap = {}
    for tag, factor in [('RevenueFromContractWithCustomerExcludingAssessedTax', 1), ('GrossProfit', .6),
                        ('OperatingIncomeLoss', .2), ('NetIncomeLoss', .1),
                        ('NetCashProvidedByUsedInOperatingActivities', .3), ('PaymentsToAcquirePropertyPlantAndEquipment', .05)]:
        gaap[tag] = {'units': {'USD': [dict(start=s, end=e, filed=f, val=v*factor,
            accn=f'0000000001-{f[:4][-2:]}-000001', form='10-K' if '12-31' in e else '10-Q') for s,e,f,v in periods]}}
    for tag, values in [('StockholdersEquity', [100,120]), ('AssetsCurrent', [80,100]), ('LiabilitiesCurrent', [40,50])]:
        gaap[tag] = {'units': {'USD': [dict(end=e, filed=f, val=v, form='10-Q', accn='0000000001-26-000001')
            for e,f,v in zip(['2025-06-30','2026-06-30'],['2025-08-01','2026-08-01'],values)]}}
    return {'cik': 1, 'facts': {'us-gaap': gaap}}, {'cik': '1', 'name': 'Sample', 'tickers': ['ABC'], 'exchanges': ['NYSE']}


def project(facts, company=None, **kw):
    return project_fundamentals(json.dumps(facts).encode(), json.dumps(company or sources()[1]).encode(),
        symbol='ABC', cik='1', observed_at=kw.get('now', NOW))


def test_ttm_math_and_exact_source_provenance():
    result = project(*sources())
    assert result['revenue_growth'] == pytest.approx((130/110-1)*100)
    assert result['roe'] == pytest.approx(13/110*100)
    assert result['operating_margin'] == 20
    assert result['current_ratio'] == 2
    assert result['free_cash_flow'] == 32.5
    evidence = json.loads(result['source_evidence_json'])
    assert evidence['current']['revenue']['value'] == 130
    assert evidence['period_start'] == '2025-07-01'
    assert len(evidence['current']['revenue']['inputs']) == 3
    assert evidence['available_by'] == '2026-10-07'
    assert all('accession' in row for row in evidence['current']['revenue']['inputs'])
    assert 'forward_pe' not in result and 'ev_to_ebitda' not in result and 'eps_ttm' not in result


def test_restatement_availability_does_not_use_same_day_or_future_filings():
    facts, _ = sources()
    rows = facts['facts']['us-gaap']['RevenueFromContractWithCustomerExcludingAssessedTax']['units']['USD']
    row = {**rows[-1], 'filed': '2026-10-08', 'val': 999, 'form': '10-Q/A'}
    rows.append(row)
    assert json.loads(project(facts)['source_evidence_json'])['current']['revenue']['value'] == 130
    next_day = NOW.replace(day=9)
    assert json.loads(project(facts, now=next_day)['source_evidence_json'])['current']['revenue']['value'] == 1059
    # UTC midnight is still the prior New York filing day.
    assert json.loads(project(facts, now=next_day.replace(hour=2))['source_evidence_json'])['current']['revenue']['value'] == 130


def test_wrong_currency_period_and_nonfinite_inputs_are_missing():
    facts, _ = sources()
    gaap = facts['facts']['us-gaap']
    gaap['GrossProfit']['units']['EUR'] = gaap['GrossProfit']['units'].pop('USD')
    gaap['OperatingIncomeLoss']['units']['USD'][-1]['start'] = '2026-04-01'
    gaap['NetIncomeLoss']['units']['USD'][-1]['val'] = float('nan')
    result = project(facts)
    assert all(name not in result for name in ('gross_margin', 'operating_margin', 'roe'))
    assert result['current_ratio'] == 2


def test_conflicting_equal_date_facts_and_incomplete_latest_period_are_held():
    facts, _ = sources()
    rows = facts['facts']['us-gaap']['RevenueFromContractWithCustomerExcludingAssessedTax']['units']['USD']
    rows.append({**rows[-1], 'val': 999})
    with pytest.raises(SecFundamentalsError):
        project(facts)


@pytest.mark.parametrize('kind', ['cik', 'symbol', 'old', 'missing_ytd'])
def test_invalid_identity_or_stale_and_incomplete_coverage_is_rejected(kind):
    facts, company = sources()
    if kind == 'cik': company['cik'] = 2
    if kind == 'symbol': company['tickers'] = ['XYZ']
    if kind == 'missing_ytd':
        rows = facts['facts']['us-gaap']['RevenueFromContractWithCustomerExcludingAssessedTax']['units']['USD']
        rows[-1]['start'] = '2026-04-01'
    with pytest.raises(SecFundamentalsError):
        project(facts, company, now=NOW.replace(year=2027) if kind == 'old' else NOW)


def test_negative_equity_does_not_create_bullish_roe():
    facts, _ = sources()
    facts['facts']['us-gaap']['StockholdersEquity']['units']['USD'][-1]['val'] = -20
    assert 'roe' not in project(facts)


def test_total_revenue_is_not_confused_with_contract_revenue_component():
    facts, _ = sources()
    gaap = facts['facts']['us-gaap']
    gaap['Revenues'] = deepcopy(gaap['RevenueFromContractWithCustomerExcludingAssessedTax'])
    for row in gaap['Revenues']['units']['USD']:
        row['val'] *= 2
    evidence = json.loads(project(facts)['source_evidence_json'])
    assert evidence['current']['revenue']['value'] == 260
    assert {r['tag'] for r in evidence['current']['revenue']['inputs']} == {'Revenues'}


def test_selected_provider_refresh_persists_evidence_and_skips_fmp(monkeypatch):
    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session
    from app.db import Base, ensure_fundamentals_cache_schema
    from app.models import FundamentalsCache, FundamentalsSnapshot
    from app.services import fundamentals_cache as cache, sec_fundamentals as sec
    from app.clients.direct_sources import DirectSourceClient
    import requests
    monkeypatch.setenv('FUNDAMENTALS_PROVIDER', 'sec_edgar')
    monkeypatch.setenv('STOCK_PRICE_PROVIDER', 'fmp')  # independent market enrichment stays off
    monkeypatch.setenv('FMP_PROVIDER_DISABLED', '1')
    monkeypatch.setattr(requests, 'get', lambda *a, **k: pytest.fail('Unexpected transport'))
    facts, company = sources()
    monkeypatch.setattr(sec, 'company_directory', lambda day: {'ABC': {'cik': '0000000001'}})
    monkeypatch.setattr(DirectSourceClient, 'get', lambda self,url: json.dumps(facts if 'companyfacts' in url else company).encode())
    # Keep the fixture clock fixed without modifying shared datetime classes.
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None): return NOW
    monkeypatch.setattr(sec, 'datetime', Clock)
    result = cache.fetch_fundamentals_for_symbol('ABC')
    assert result.status == 'ok'
    engine = create_engine('sqlite://')
    Base.metadata.create_all(engine, tables=[FundamentalsCache.__table__, FundamentalsSnapshot.__table__])
    ensure_fundamentals_cache_schema(engine)
    ensure_fundamentals_cache_schema(engine)
    with Session(engine) as db:
        db.add(FundamentalsCache(symbol='ABC', provider='fmp', fetched_at=NOW, status='ok', roe=999))
        cache.upsert_fundamentals_cache(db, result.values)
        db.commit()
        selected = cache.cached_fundamentals_by_symbol(db, ['ABC'])['ABC']
        assert selected.provider == 'sec_edgar' and selected.roe != 999
        assert json.loads(selected.source_evidence_json)['methodology'] == sec.VERSION
        from app.services.fundamentals_snapshots import upsert_fundamentals_snapshot
        snapshot = upsert_fundamentals_snapshot(db, selected, observed_at=NOW)
        assert snapshot.source_evidence_json == selected.source_evidence_json
        assert cache.fundamentals_summary_from_cache_row(selected)['data_quality']['scored_metric_count'] == 3
        cache.upsert_fundamentals_cache(db, result.values)
        db.commit()
        assert db.query(FundamentalsCache).count() == 2
        assert len(cache.cached_screener_rows(db)) == 1
        # A fresh record with missing current market data must not retain old cap.
        cache.upsert_fundamentals_cache(db, {**result.values, 'market_cap': 123})
        db.flush()
        cache.upsert_fundamentals_cache(db, result.values)
        db.commit()
        assert selected.market_cap is None
    engine.dispose()


@pytest.mark.parametrize('coverage', ['complete', 'missing', 'stale', 'incomplete'])
def test_selected_market_fields_and_rank_inputs_use_massive_provenance(monkeypatch, coverage):
    from datetime import timedelta
    from types import SimpleNamespace
    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session
    from app.db import Base
    from app.models import FundamentalsCache
    from app.services import sec_fundamentals as sec, fundamentals_cache as cache, leaderboard_market_data as market
    from app.clients.direct_sources import DirectSourceClient
    from app.clients.massive_stocks import MassiveStocksClient
    monkeypatch.setenv('FUNDAMENTALS_PROVIDER', 'sec_edgar')
    monkeypatch.setenv('STOCK_PRICE_PROVIDER', 'massive')
    monkeypatch.setenv('FMP_PROVIDER_DISABLED', '1')
    monkeypatch.setenv('MASSIVE_API_KEY', 'test-only')
    facts, company = sources()
    if coverage == 'missing':
        facts['facts'] = {'ifrs-full': {}}
    elif coverage == 'stale':
        for concept in facts['facts']['us-gaap'].values():
            concept['units']['USD'] = [r for r in concept['units']['USD'] if r['end'] < '2025-01-01']
    elif coverage == 'incomplete':
        facts['facts']['us-gaap']['RevenueFromContractWithCustomerExcludingAssessedTax']['units']['USD'][-1]['start'] = '2026-04-01'
    monkeypatch.setattr(sec, 'company_directory', lambda day: {'ABC': {'cik': '0000000001'}})
    monkeypatch.setattr(DirectSourceClient, 'get', lambda self,url: json.dumps(facts if 'companyfacts' in url else company).encode())
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None): return NOW
    monkeypatch.setattr(sec, 'datetime', Clock)
    def transport(url, **kw):
        assert url.startswith('https://api.massive.com/')
        if '/reference/' in url:
            rows = dict(ticker='ABC', cik='1', currency_name='usd', market='stocks', locale='us', market_cap=1300)
        else:
            rows = [{'t': (NOW.replace(hour=4) - timedelta(days=d)).timestamp()*1000, 'c': 13, 'v': 100+d}
                    for d in range(20,0,-1)]
        return SimpleNamespace(status_code=200, json=lambda: {'status': 'OK', 'results': rows})
    import requests
    monkeypatch.setattr(requests, 'get', transport)
    values = sec.fetch_fundamentals('ABC')
    assert values['market_cap'] == 1300 and values['avg_volume'] == 110.5
    if coverage == 'complete':
        assert values['price_to_sales'] == 10 and values['trailing_pe'] == 100
        assert values['fcf_yield'] == 2.5
    else:
        assert values['period_date'] is None
        assert not {'price_to_sales', 'trailing_pe', 'fcf_yield', 'revenue_growth', 'roe'} & values.keys()
        evidence = json.loads(values['source_evidence_json'])
        assert evidence['financial_status'] == 'unavailable' and evidence['financial_error']
    engine = create_engine('sqlite://')
    Base.metadata.create_all(engine, tables=[FundamentalsCache.__table__])
    with Session(engine) as db:
        # A new coverage gap must clear previous financial values while keeping
        # independently verified market inputs usable by the ranking adapter.
        if coverage != 'complete':
            cache.upsert_fundamentals_cache(db, {'symbol': 'ABC', 'provider': 'sec_edgar',
                'fetched_at': NOW,
                'revenue_growth': 99, 'roe': 99, 'operating_margin_expansion': 99})
            db.flush()
        cache.upsert_fundamentals_cache(db, values)
        db.commit()
        if coverage != 'complete':
            summary = cache.fundamentals_summary_from_cache_row(db.query(FundamentalsCache).one(), now=NOW)
            assert summary['status'] == 'unavailable'
            assert summary['data_quality']['scored_metric_count'] == 0
        rows = [{'symbol': 'ABC', 'market_cap': 999999, 'avg_volume': 999999}]
        monkeypatch.setattr(requests, 'get', lambda *a, **k: pytest.fail('Ranking unexpectedly fetched a provider'))
        market.enrich_leaderboard_market_data(db, rows, now=NOW)
        assert rows[0]['market_cap'] == 1300 and rows[0]['avg_volume_source'] == 'massive_daily_bars'
        market.enrich_leaderboard_market_data(db, rows, now=NOW+timedelta(days=8))
        assert rows[0]['market_cap'] is None and rows[0]['avg_volume'] is None
    engine.dispose()


@pytest.mark.parametrize('invalid', ['company_cik', 'facts_cik', 'symbol'])
def test_market_only_fallback_never_bypasses_sec_identity(monkeypatch, invalid):
    from app.services import sec_fundamentals as sec
    from app.clients.direct_sources import DirectSourceClient
    from app.clients.massive_stocks import MassiveStocksClient
    facts, company = sources()
    facts['facts'] = {}
    if invalid == 'company_cik': company['cik'] = 2
    if invalid == 'facts_cik': facts['cik'] = 2
    if invalid == 'symbol': company['tickers'] = ['OTHER']
    monkeypatch.setattr(sec, 'company_directory', lambda day: {'ABC': {'cik': '0000000001'}})
    monkeypatch.setattr(DirectSourceClient, 'get', lambda self,url: json.dumps(facts if 'companyfacts' in url else company).encode())
    monkeypatch.setenv('STOCK_PRICE_PROVIDER', 'massive')
    monkeypatch.setattr(MassiveStocksClient, 'details', lambda *a: pytest.fail('Identity failure reached market lookup'))
    with pytest.raises(SecFundamentalsError, match='identity'):
        sec.fetch_fundamentals('ABC')


def test_legacy_fundamentals_schema_upgrade_is_repeatable():
    from sqlalchemy import create_engine, text, inspect
    from app.db import ensure_fundamentals_cache_schema
    engine = create_engine('sqlite://')
    with engine.begin() as connection:
        connection.execute(text('CREATE TABLE fundamentals_cache (id INTEGER PRIMARY KEY, symbol TEXT, provider TEXT, fetched_at TIMESTAMP, updated_at TIMESTAMP)'))
        connection.execute(text("INSERT INTO fundamentals_cache VALUES (1,'ABC','fmp','2026-10-07','2026-10-07')"))
    ensure_fundamentals_cache_schema(engine)
    ensure_fundamentals_cache_schema(engine)
    assert 'source_evidence_json' in {c['name'] for c in inspect(engine).get_columns('fundamentals_cache')}
    with engine.connect() as connection:
        assert connection.execute(text('SELECT symbol, source_evidence_json FROM fundamentals_cache')).one() == ('ABC', None)
    engine.dispose()


def test_reported_revenue_including_tax_retains_its_exact_concept_and_ttm_inputs():
    facts,company=sources()
    gaap=facts['facts']['us-gaap']
    inclusive='RevenueFromContractWithCustomerIncludingAssessedTax'
    gaap[inclusive]=gaap.pop('RevenueFromContractWithCustomerExcludingAssessedTax')
    result=project(facts,company)
    assert result['revenue_growth']==pytest.approx((130/110-1)*100)
    assert result['free_cash_flow']==32.5
    evidence=json.loads(result['source_evidence_json'])
    assert {r['tag'] for r in evidence['current']['revenue']['inputs']}=={inclusive}
    assert {r['tag'] for r in evidence['prior']['revenue']['inputs']}=={inclusive}


def test_current_net_revenue_keeps_priority_over_inclusive_tax_concept():
    facts,company=sources()
    gaap=facts['facts']['us-gaap']
    inclusive=deepcopy(gaap['RevenueFromContractWithCustomerExcludingAssessedTax'])
    for row in inclusive['units']['USD']:row['val']*=10
    gaap['RevenueFromContractWithCustomerIncludingAssessedTax']=inclusive
    result=project(facts,company)
    evidence=json.loads(result['source_evidence_json'])
    assert evidence['current']['revenue']['value']==130
    assert {r['tag'] for r in evidence['current']['revenue']['inputs']}=={'RevenueFromContractWithCustomerExcludingAssessedTax'}


def test_new_inclusive_tax_series_cannot_borrow_annual_net_revenue_history():
    facts,company=sources()
    gaap=facts['facts']['us-gaap']
    rows=gaap['RevenueFromContractWithCustomerExcludingAssessedTax']['units']['USD']
    latest=rows.pop()
    gaap['RevenueFromContractWithCustomerIncludingAssessedTax']={'units':{'USD':[latest]}}
    with pytest.raises(SecFundamentalsError,match='complete trailing revenue'):
        project(facts,company)
