from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
import gzip
import hashlib
import json
import socket

import pytest

from app.services import sec_reporting_scope as scope
from app.services import sec_fundamentals as sec
from app.services import sec_financial_statements as statements

ROOT = Path(__file__).parent / 'fixtures/sec_reporting_scope'
NOW = datetime(2026, 11, 17, 21, tzinfo=timezone.utc)


def originals():
    manifest = json.loads((ROOT / 'sources.json').read_bytes())
    result = {}
    for name, meta in manifest.items():
        raw = gzip.decompress((ROOT / meta['file']).read_bytes())
        assert len(raw) == meta['bytes']
        assert hashlib.sha256(raw).hexdigest() == meta['sha256']
        result[name] = raw
    return manifest, result


def project(raw):
    return sec.project_fundamentals(raw['facts'], raw['company'], symbol='NFE',
        cik=scope.CIK, observed_at=NOW)


def test_complete_originals_bind_the_scope_change_without_pro_forma_substitution():
    from lxml import html
    manifest, raw = originals()
    for name, (url, digest) in scope.SOURCES.items():
        assert manifest[name]['url'] == url
        assert hashlib.sha256(raw[name]).hexdigest() == digest
    text = ' '.join(html.fromstring(raw['current']).text_content().split())
    assert 'discontinued operations for all periods presented' in text
    separation = html.fromstring(raw['separation'])
    tables = [' '.join(t.text_content().split()) for t in separation.xpath('//table')]
    assert any('1,504,037' in t and '1,300,165' in t and '1,258,876' in t
        and 'Transaction accounting adjustments' in t for t in tables)
    result = project(raw)
    evidence = json.loads(result['source_evidence_json'])
    assert evidence['current']['revenue'] is None
    assert evidence['reporting_scope'] == scope.policy(scope.CIK, NOW.date())
    assert result['free_cash_flow'] == -589920000
    assert result['current_ratio'] == pytest.approx(891482 / 989560)
    assert not {'revenue_growth', 'operating_margin', 'operating_margin_expansion',
        'net_margin', 'fcf_margin', 'roe'} & result.keys()


def test_actual_records_hold_mixed_ttm_and_derived_q4_but_keep_reported_periods():
    _, raw = originals()
    payload = json.loads(raw['facts'])
    records = sec._records(payload, 'revenue', NOW.date())
    assert sec._ttm(records, date(2026, 9, 30)) is None
    annual = sec._ttm(records, date(2025, 12, 31))
    assert annual['value'] == 1504037000
    assert statements.period_value(records, date(2025, 10, 1), date(2025, 12, 31)) is None
    assert statements.period_value(records, date(2026, 7, 1), date(2026, 9, 30))['value'] == 335818000
    unscoped = {key: {k: v for k, v in point.items() if k != 'reporting_scope'}
        if point else point for key, point in records.items()}
    assert sec._ttm(unscoped, date(2026, 9, 30))['value'] == 1243087000


def fact(accession=scope.ACCESSION, filed=scope.FILED, tag='Revenues'):
    return {'tag': tag, 'accession': accession, 'filed': filed, 'value': 10}


@pytest.mark.parametrize('cik,available,namespace', [
    ('1', NOW.date(), 'us-gaap'),
    (scope.CIK, date(2026, 11, 15), 'us-gaap'),
    (scope.CIK, NOW.date(), 'ifrs-full'),
])
def test_other_issuers_past_availability_and_other_taxonomies_are_unchanged(cik, available, namespace):
    records = {('start', 'end'): fact()}
    assert scope.annotate(records, cik=cik, available_by=available, namespace=namespace) is records


@pytest.mark.parametrize('tag', ['NetCashProvidedByUsedInOperatingActivities',
    'PaymentsToAcquirePropertyPlantAndEquipment', 'AssetsCurrent', 'NetIncomeLoss'])
def test_independently_reported_cash_balances_and_parent_income_are_not_rewritten(tag):
    records = {(None, NOW.date()): fact(tag=tag)}
    assert scope.annotate(records, cik=scope.CIK, available_by=NOW.date(), namespace='us-gaap') == records


def annotated(point):
    return scope.annotate({1: point}, cik=scope.CIK, available_by=NOW.date(), namespace='us-gaap')[1]


def test_missing_conflicting_and_future_scope_proofs_do_not_bridge():
    current = annotated(fact())
    historical = annotated(fact(accession='0001749723-26-000032', filed='2026-04-13'))
    future = annotated(fact(accession='0001749723-27-000001', filed='2027-03-01'))
    assert scope.compatible([current, deepcopy(current)])
    assert scope.compatible([future, deepcopy(future)])
    for other in [historical, future, fact(), None,
                  {**current, 'reporting_scope': {**current['reporting_scope'], 'policy': 'other'}}]:
        assert not scope.compatible([current, other])
    assert not scope.comparable({'inputs': [current]}, {'inputs': [historical]})
    assert scope.comparable({'inputs': [historical]}, {'inputs': [historical]})


def test_complete_reader_requests_and_values_repeat_without_network(monkeypatch):
    manifest, raw = originals()
    urls = {meta['url']: raw[name] for name, meta in manifest.items()}
    calls = []
    class Client:
        def get(self, url):
            calls.append(url)
            return urls[url]
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return NOW
    monkeypatch.setattr(socket.socket, 'connect', lambda *a, **k: pytest.fail('Unexpected network'))
    monkeypatch.setattr(sec, 'datetime', Clock)
    monkeypatch.setattr(sec, 'company_directory', lambda _: {'NFE': {'cik': scope.CIK}})
    monkeypatch.setenv('STOCK_PRICE_PROVIDER', 'fmp')
    monkeypatch.setenv('SEC_FINANCIAL_DISCOVERY_READ_ENABLED', '0')
    monkeypatch.setattr('app.clients.direct_sources.DirectSourceClient', Client)
    first = sec.fetch_fundamentals('NFE')
    first_calls = list(calls)
    calls.clear()
    assert sec.fetch_fundamentals('NFE') == first
    assert calls == first_calls == [manifest[k]['url'] for k in ('company', 'facts', 'current')]
    assert first['free_cash_flow'] == -589920000 and 'fcf_margin' not in first


def test_historical_panel_keeps_reported_values_rejects_legacy_cache_and_repeats():
    _, raw = originals()
    args = dict(symbol='NFE', cik=scope.CIK, observed_at=NOW)
    panel = statements.project(raw['facts'], raw['company'], **args)
    assert statements.project(raw['facts'], raw['company'], **args) == panel
    assert any(r['date'] == '2025-12-31' and r['revenue'] == 1504037000 for r in panel['annual'])
    assert not any(r['date'] == '2025-12-31' for r in panel['quarterly'])
    assert any(r['date'] == '2026-09-30' and r['revenue'] == 335818000 for r in panel['quarterly'])
    row = SimpleNamespace(source='sec_edgar', fetched_at=NOW, payload_json=json.dumps(panel))
    assert statements.cached_row_payload(row, 'NFE', now=NOW) == panel
    for field in ('preparationVersion', 'reportingScope'):
        old = deepcopy(panel)
        (old if field == 'preparationVersion' else old['sourceEvidence']).pop(field)
        row.payload_json = json.dumps(old)
        assert statements.cached_row_payload(row, 'NFE', now=NOW) is None


def test_new_preparation_version_is_scoped_to_nfe():
    from app.services import sec_fundamentals_preparation as prep
    assert scope.VERSION in prep.version_for_symbol('NFE')
    assert scope.VERSION not in prep.version_for_symbol('NFEGP')
    assert scope.VERSION not in prep.version_for_symbol('VMRK')


def test_preparation_and_consumers_clear_held_values_without_sending_alerts(monkeypatch):
    from sqlalchemy import create_engine, select, func
    from sqlalchemy.orm import sessionmaker
    from app.db import Base
    from app.models import (FundamentalsCache, Event, EmailDelivery, MonitoringAlert,
        Security, UserAccount, Watchlist, WatchlistItem, WatchlistAlertRule)
    from app.services import sec_fundamentals_preparation as prep
    from app.services.fundamentals_cache import (upsert_fundamentals_cache,
        fundamentals_summary_from_cache_row, cache_row_to_screener_row)
    from app.services.custom_alert_rules import evaluate_watchlist_custom_alerts, validate_conditions
    manifest, raw = originals()
    urls = {meta['url']: raw[name] for name, meta in manifest.items()}
    calls = []
    class Client:
        def get(self, url):
            calls.append(url)
            return urls[url]
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return NOW
    monkeypatch.setattr(socket.socket, 'connect', lambda *a, **k: pytest.fail('Unexpected network'))
    engine = create_engine('sqlite://')
    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine)
    monkeypatch.setattr(prep, 'SessionLocal', factory)
    monkeypatch.setattr(prep, 'datetime', Clock)
    monkeypatch.setattr(prep, 'DirectSourceClient', Client)
    monkeypatch.setattr(prep.sec_directory, 'directory', lambda: {'NFE': {'cik': scope.CIK}})
    monkeypatch.setenv('SEC_FUNDAMENTALS_WARMING_ENABLED', '1')
    payload = prep.prepare('NFE')
    request_count = len(calls)
    assert prep.prepare('NFE') == payload and len(calls) == request_count
    assert set(payload['values']) == {'current_ratio', 'free_cash_flow'}
    with factory() as db:
        assert db.scalar(select(func.count()).select_from(FundamentalsCache)) == 0
        assert db.scalar(select(func.count()).select_from(Event)) == 0
        user = UserAccount(email='scope@example.test')
        security = Security(symbol='NFE', name='NFE', asset_class='equity')
        db.add_all([user, security]); db.flush()
        watch = Watchlist(name='Scope proof', owner_user_id=user.id)
        db.add(watch); db.flush()
        db.add(WatchlistItem(watchlist_id=watch.id, security_id=security.id, target_type='ticker'))
        rule = WatchlistAlertRule(user_id=user.id, watchlist_id=watch.id, name='Revenue growth',
            enabled=True, scope_type='any_watchlist_ticker', match_type='all', delivery='both',
            conditions_json=json.dumps(validate_conditions([dict(metric='revenue_growth', operator='lt',
                comparison_type='value', comparison_value=0)])))
        db.add(rule)
        old = {**project(raw), 'fcf_margin': -47.45605094414148, 'revenue_growth': -26.50826354668881,
            'operating_margin': -119.59484734374988, 'operating_margin_expansion': -108.30422057937697,
            'source_evidence_json': json.dumps({'methodology': 'sec_fundamentals_v1'})}
        upsert_fundamentals_cache(db, old); db.commit()
        monkeypatch.setenv('FUNDAMENTALS_PROVIDER', 'sec_edgar')
        def run(minutes):
            result = evaluate_watchlist_custom_alerts(db, user_id=user.id, watchlist_id=watch.id,
                now=NOW + timedelta(minutes=minutes))
            db.commit()
            return result
        assert run(0)['triggered'] == 0
        replacement = project(raw)
        upsert_fundamentals_cache(db, replacement); db.commit()
        row = db.scalar(select(FundamentalsCache).where(FundamentalsCache.symbol == 'NFE'))
        for metric in ('fcf_margin', 'revenue_growth', 'operating_margin', 'operating_margin_expansion'):
            assert getattr(row, metric) is None
        assert row.free_cash_flow == -589920000
        summary = fundamentals_summary_from_cache_row(row, now=NOW)
        assert summary['status'] == 'unavailable'
        assert cache_row_to_screener_row(row)['symbol'] == 'NFE'
        assert run(5)['triggered'] == 0 and run(10)['triggered'] == 0
        assert db.scalar(select(func.count()).select_from(MonitoringAlert)) == 0
        assert db.scalar(select(func.count()).select_from(EmailDelivery)) == 0
    engine.dispose()
