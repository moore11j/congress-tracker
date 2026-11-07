"""A rejected statement never invalidates independent, attributed ratios."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
import gzip
import hashlib
import json
import socket

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.models import FundamentalsCache
from app.clients.direct_sources import DirectSourceError
from app.services import sec_fundamentals as sec, free_fundamentals as free
from app.services import finnhub_metrics, fundamentals_cache as cache
from app.services import sec_inline_financials as inline
from app.services.independent_fundamentals import IndependentMetricsEligible
from app.services.sec_tabular_reporting import TabularReportingError
from app.services.sec_tagged_foreign import TaggedCurrencyError
from app.services.sec_statement_currency import StatementCurrencyError
from app.services.finnhub_research import FinnhubUnavailable
from test_finnhub_metrics import payload

NOW = datetime(2026, 11, 7, 6, tzinfo=timezone.utc)
COMPANY = {'cik': 1, 'name': 'Independent Example', 'tickers': ['ABC'], 'exchanges': ['NYSE']}
FACTS = {'cik': 1, 'facts': {'us-gaap': {'UnqualifiedAmount': {'units': {'USD': [{'val': 999999}]}}}}}


@pytest.fixture
def source(monkeypatch):
    monkeypatch.setattr(socket.socket, 'connect', lambda *a, **k: pytest.fail('Unexpected network'))
    monkeypatch.setenv('FUNDAMENTALS_PROVIDER', 'free_direct')
    monkeypatch.setenv('STOCK_PRICE_PROVIDER', 'fmp')
    monkeypatch.setattr(sec, 'company_directory', lambda _: {'ABC': {'cik': '0000000001'}})
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None): return NOW
    monkeypatch.setattr(sec, 'datetime', Clock)
    calls = []
    source = {'company': deepcopy(COMPANY), 'facts': deepcopy(FACTS), 'error': None}
    def get(self, url):
        calls.append(url)
        if '/submissions/' in url: return json.dumps(source['company']).encode()
        if source['error']: raise source['error']
        return json.dumps(source['facts']).encode()
    monkeypatch.setattr('app.clients.direct_sources.DirectSourceClient.get', get)
    def denied(*a, **k): raise TabularReportingError('financial_discovery_pending')
    monkeypatch.setattr(inline, 'acquire', denied)
    monkeypatch.setattr(finnhub_metrics, 'prepare', lambda symbol:
        finnhub_metrics.normalize(payload(), symbol=symbol, observed_at=NOW))
    monkeypatch.setattr(cache, '_request_rows', lambda *a, **k: pytest.fail('FMP request'))
    monkeypatch.setattr(cache, 'fetch_company_profile', lambda *a, **k: pytest.fail('FMP profile'))
    return source, calls


@pytest.mark.parametrize('kind', [TabularReportingError, TaggedCurrencyError, StatementCurrencyError])
@pytest.mark.parametrize('stage', ['acquire', 'project'])
def test_explicit_financial_failure_retains_only_verified_independent_metrics(source, monkeypatch, kind, stage):
    def reject(*a, **k): raise kind('unqualified_statement')
    monkeypatch.setattr(inline, 'acquire', reject if stage == 'acquire' else lambda *a, **k: None)
    if stage == 'project': monkeypatch.setattr(sec, 'project_fundamentals', reject)
    result = free.fetch_fundamentals('ABC')
    assert result['roe'] == 25 and result['ev_to_ebitda'] == 18 and result['revenue_growth'] == 12
    assert result['period_date'] is None and result['company_name'] == COMPANY['name']
    assert not {'free_cash_flow', 'net_debt_to_ebitda', 'price', 'market_cap'} & result.keys()
    evidence = json.loads(result['source_evidence_json'])
    assert evidence['financial_status'] == 'unavailable'
    assert evidence['current'] == evidence['prior'] == evidence['instant'] == evidence['calculations'] == {}
    assert evidence['supplement']['fields']['roe']['period_date'] == '2026-06-30'
    assert evidence['supplement']['fields']['ev_to_ebitda']['period_date'] is None
    assert len(source[1]) == 2
    assert free.fetch_fundamentals('ABC') == result
    # SEC-only readers must still fail, never label Finnhub numbers as SEC.
    with pytest.raises(IndependentMetricsEligible): sec.fetch_fundamentals('ABC')


@pytest.mark.parametrize('mutation', ['cik', 'ticker', 'duplicate_ticker', 'name', 'facts_cik'])
def test_bad_identity_cannot_reach_supplement(source, monkeypatch, mutation):
    row = source[0]
    if mutation == 'facts_cik': row['facts']['cik'] = 2
    elif mutation == 'cik': row['company']['cik'] = 2
    elif mutation == 'name': row['company']['name'] = ''
    else: row['company']['tickers'] = ['OTHER'] if mutation == 'ticker' else ['ABC', 'ABC']
    monkeypatch.setattr(finnhub_metrics, 'prepare', lambda *_: pytest.fail('Unverified identity'))
    with pytest.raises((sec.SecFundamentalsError, DirectSourceError)): free.fetch_fundamentals('ABC')


@pytest.mark.parametrize('kind', [ValueError, inline.InlineFactsError, RuntimeError, DirectSourceError])
def test_other_failures_are_not_masked(source, monkeypatch, kind):
    def reject(*a, **k): raise kind('other_failure')
    monkeypatch.setattr(inline, 'acquire', reject)
    monkeypatch.setattr(finnhub_metrics, 'prepare', lambda *_: pytest.fail('Unexpected fallback'))
    with pytest.raises(kind, match='other_failure'): free.fetch_fundamentals('ABC')


@pytest.mark.parametrize('status', [403, 404, 429, 500])
def test_only_exact_companyfacts_not_found_is_eligible(source, status):
    source[0]['error'] = DirectSourceError(f'Source HTTP {status}: https://data.sec.gov/api/xbrl/companyfacts/CIK0000000001.json')
    if status == 404:
        result = free.fetch_fundamentals('ABC')
        assert result['roe'] == 25
        assert 'facts' not in json.loads(result['source_evidence_json'])
    else:
        with pytest.raises(DirectSourceError): free.fetch_fundamentals('ABC')


@pytest.mark.parametrize('failure', ['empty', 'stale', 'unavailable', 'identity'])
def test_missing_independent_metrics_does_not_save_empty_success(source, monkeypatch, failure):
    def prepare(symbol):
        if failure in {'unavailable', 'identity'}: raise FinnhubUnavailable('rate_limited' if failure == 'unavailable' else 'metric_symbol_mismatch')
        raw = payload()
        if failure == 'empty': raw['series'] = {}
        return finnhub_metrics.normalize(raw, symbol=symbol,
            observed_at=NOW if failure == 'empty' else NOW.replace(year=2027))
    monkeypatch.setattr(finnhub_metrics, 'prepare', prepare)
    with pytest.raises(IndependentMetricsEligible): free.fetch_fundamentals('ABC')


def test_exact_cache_repeat_provenance_summary_and_screener(source):
    engine = create_engine('sqlite://')
    Base.metadata.create_all(engine)
    with sessionmaker(bind=engine)() as db:
        legacy = FundamentalsCache(symbol='ABC', provider='fmp', roe=99, fetched_at=NOW, status='ok')
        db.add(legacy); db.commit()
        result = cache.fetch_fundamentals_for_symbol('ABC')
        assert result.status == 'ok'
        cache.upsert_fundamentals_cache(db, result.values); db.commit()
        row = db.scalar(select(FundamentalsCache).where(FundamentalsCache.provider == 'free_direct'))
        summary = cache.fundamentals_summary_from_cache_row(row, now=NOW)
        assert summary['data_quality']['available']
        assert summary['metrics']['return_on_equity']['value'] == .25
        assert summary['context']['source_label'] == 'Finnhub'
        assert 'SEC financial values are unavailable' in summary['context']['source_note']
        assert cache.cache_row_to_screener_row(row)['ev_ebitda'] == 18
        before = row.updated_at
        cache.upsert_fundamentals_cache(db, result.values); db.commit()
        assert row.updated_at == before and legacy.roe == 99
    engine.dispose()


def test_complete_original_pending_discovery_and_independent_source(monkeypatch):
    from app.services import sec_financial_discovery as discovery
    from app.services import sec_financial_discovery_store as store
    from test_sec_ifrs_declarations import sources
    _, args, originals, _ = sources('ONON')
    root = Path(__file__).parent / 'fixtures/independent_fundamentals'
    manifest = json.loads((root / 'sources.json').read_text())
    now = datetime.fromisoformat(manifest['observed_at'])
    def read(name):
        meta = next(r for r in manifest['sources'] if r['file'] == name)
        raw = gzip.decompress((root / name).read_bytes())
        assert len(raw) == meta['bytes'] and hashlib.sha256(raw).hexdigest() == meta['sha256']
        return raw
    company, facts = args['company_raw'], read('ONON-facts.json.gz')
    cik = args['cik']
    metadata = json.loads(company)
    checkpoint = discovery.inspect(company, symbol='ONON', cik=cik,
        available_by=now.date(), observed_at=now)['state']
    assert checkpoint['receipts'] == {}
    monkeypatch.setattr(socket.socket, 'connect', lambda *a, **k: pytest.fail('Network'))
    monkeypatch.setenv('SEC_FINANCIAL_DISCOVERY_READ_ENABLED', '1')
    monkeypatch.setattr(store, 'read', lambda *a: deepcopy(checkpoint))
    monkeypatch.setattr(sec, 'company_directory', lambda _: {'ONON': {'cik': cik}})
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None): return now
    monkeypatch.setattr(sec, 'datetime', Clock)
    calls = []
    def get(self, url):
        calls.append(url)
        if url == f'https://data.sec.gov/submissions/CIK{cik}.json': return company
        if url == f'https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json': return facts
        pytest.fail('Pending discovery must not acquire rejected financial statements')
    monkeypatch.setattr('app.clients.direct_sources.DirectSourceClient.get', get)
    original = json.loads(read('ONON-metrics.json.gz'))
    normalized = finnhub_metrics.normalize(original, symbol='ONON', observed_at=now)
    monkeypatch.setattr(finnhub_metrics, 'prepare', lambda _: deepcopy(normalized))
    first = free.fetch_fundamentals('ONON')
    assert first['company_name'] == metadata['name'] and first['period_date'] is None
    assert {k: first[k] for k in normalized['values']} == normalized['values']
    assert len(calls) == 2
    proof = json.loads(first['source_evidence_json'])
    assert proof['financial_error'] == 'financial_discovery_pending'
    assert proof['current'] == proof['instant'] == proof['calculations'] == {}
    assert not {'free_cash_flow', 'net_debt_to_ebitda', 'operating_margin_expansion'} & first.keys()
    assert free.fetch_fundamentals('ONON') == first and len(calls) == 4
    with pytest.raises(IndependentMetricsEligible, match='financial_discovery_pending'):
        sec.fetch_fundamentals('ONON')


def test_complete_original_success_remains_identical(monkeypatch):
    from test_sec_primary_layout import Client, NOW as observed, CIK
    monkeypatch.setattr(socket.socket, 'connect', lambda *a, **k: pytest.fail('Network'))
    monkeypatch.setenv('SEC_FINANCIAL_DISCOVERY_READ_ENABLED', '0')
    monkeypatch.setenv('STOCK_PRICE_PROVIDER', 'fmp')
    monkeypatch.setattr(sec, 'company_directory', lambda _: {'QCOM': {'cik': CIK}})
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None): return observed
    monkeypatch.setattr(sec, 'datetime', Clock)
    client = Client()
    monkeypatch.setattr('app.clients.direct_sources.DirectSourceClient', lambda: client)
    first = sec.fetch_fundamentals('QCOM')
    assert first == sec.fetch_fundamentals('QCOM')
    proof = json.loads(first['source_evidence_json'])
    assert proof['leverage_preparation']['ebitda']['value'] == 11733000000
    assert 'independent_metrics' not in proof
    assert first.get('net_debt_to_ebitda') is None


def test_actual_independent_result_rebaselines_watchlist_without_false_or_duplicate_alert(source):
    from app.models import (MonitoringAlert, Security, UserAccount, Watchlist, WatchlistItem,
                            WatchlistAlertRule)
    from app.services.custom_alert_rules import evaluate_watchlist_custom_alerts, validate_conditions
    engine = create_engine('sqlite://'); Base.metadata.create_all(engine)
    with sessionmaker(bind=engine)() as db:
        user = UserAccount(email='independent@example.test')
        security = Security(symbol='ABC', name='Independent Example', asset_class='equity')
        db.add_all([user, security]); db.flush()
        watchlist = Watchlist(name='Independent', owner_user_id=user.id)
        db.add(watchlist); db.flush()
        db.add(WatchlistItem(watchlist_id=watchlist.id, security_id=security.id, target_type='ticker'))
        rule = WatchlistAlertRule(user_id=user.id, watchlist_id=watchlist.id, name='Liquidity',
            enabled=True, scope_type='any_watchlist_ticker', match_type='all', delivery='both',
            conditions_json=json.dumps(validate_conditions([dict(metric='current_ratio',
                operator='gt', comparison_type='value', comparison_value=1.5)])))
        row = FundamentalsCache(symbol='ABC', provider='free_direct', status='ok', fetched_at=NOW,
            current_ratio=1, source_evidence_json=json.dumps({'methodology': free.VERSION}))
        db.add_all([row, rule]); db.commit()
        def run(minutes):
            result = evaluate_watchlist_custom_alerts(db, user_id=user.id,
                watchlist_id=watchlist.id, now=NOW+timedelta(minutes=minutes))
            db.commit(); return result
        assert run(0)['triggered'] == 0
        result = free.fetch_fundamentals('ABC')
        cache.upsert_fundamentals_cache(db, result); db.commit()
        assert row.current_ratio == 2
        first = run(5)
        assert first['triggered'] == 0 and first['initialized'] == 1
        assert run(10)['triggered'] == 0
        assert not db.scalars(select(MonitoringAlert)).all()
        # A subsequent real value transition with the same source basis is
        # still actionable. It is not hidden by repeated source observations.
        row.current_ratio = 1; db.commit(); assert run(15)['triggered'] == 0
        row.current_ratio = 2; db.commit(); assert run(20)['triggered'] == 1
        assert run(25)['triggered'] == 0
        assert len(db.scalars(select(MonitoringAlert)).all()) == 1
    engine.dispose()
