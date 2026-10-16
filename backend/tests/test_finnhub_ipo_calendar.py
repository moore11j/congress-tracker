"""Actual public source replay and IPO calendar/digest contract acceptance."""
from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
import json
from pathlib import Path

import pytest
from sqlalchemy import select
from app.models import Event, InsightsSnapshot, NotificationSubscription
from app.services import finnhub_ipo_calendar as ipo, free_calendar, data_enrichment_queue
from app.services import event_calendar as calendar, email_digests as digests
from app.services.finnhub_research import FinnhubUnavailable
from test_email_digests import _session, _user, _watchlist

NOW = datetime(2026, 10, 16, 1, 45, tzinfo=timezone.utc)
MONTH = '2026-10'
ORIGINAL = json.loads((Path(__file__).parent/'fixtures/finnhub_ipo_oct2026.json').read_text())


@pytest.fixture
def db(monkeypatch):
    monkeypatch.setenv('CALENDAR_PROVIDER', 'free_direct')
    monkeypatch.setenv('IPO_CALENDAR_PROVIDER', 'finnhub')
    monkeypatch.setenv('FINNHUB_SHARED_LIMITER_ENABLED', '1')
    monkeypatch.setenv('FMP_PROVIDER_DISABLED', '1')
    monkeypatch.setenv('FREE_RESEARCH_WARMING_ENABLED', '0')
    monkeypatch.setattr(ipo.transport, 'request_json', lambda *a, **k: pytest.fail('Unexpected network'))
    monkeypatch.setattr(calendar, 'request_fmp_json', lambda *a, **k: pytest.fail('FMP request'))
    monkeypatch.setattr(digests, '_send_digest', lambda *a, **k: pytest.fail('Email send'))
    monkeypatch.setattr(data_enrichment_queue, 'enqueue_data_enrichment_job', lambda **k: pytest.fail('Unexpected enqueue'))
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return NOW if tz else NOW.replace(tzinfo=None)
    monkeypatch.setattr(ipo, '_now', lambda value=None: value or NOW)
    monkeypatch.setattr(free_calendar, 'datetime', Clock)
    session = _session()
    yield session
    session.close()


def save(db, raw=ORIGINAL, month=MONTH, observed=NOW):
    normalized = ipo.normalize(raw, month, observed_at=observed)
    row = InsightsSnapshot(kind=ipo.key(month), source=ipo.VERSION, fetched_at=observed,
        payload_json=json.dumps({'raw': raw, 'normalized': normalized}))
    db.add(row)
    db.commit()
    return row, normalized


def test_all_32_original_rows_preserve_source_fields_and_statuses():
    result = ipo.normalize(ORIGINAL, MONTH, observed_at=NOW)
    assert len(result['items']) == 32
    assert {i['payload']['status'] for i in result['items']} == {'expected', 'priced', 'filed', 'withdrawn'}
    source = {(r['name'], r['status']): r for r in ORIGINAL['ipoCalendar']}
    for item in result['items']:
        row = source[(item['company'], item['payload']['status'])]
        assert item['date'] == row['date'] and item['symbol'] == (row['symbol'] or None)
        for field in ('status', 'exchange', 'price', 'numberOfShares', 'totalSharesValue'):
            assert item['payload'][field] == (None if field in ('price', 'exchange') and row[field] == '' else row[field])
        assert item['payload']['currency'] is None and item['payload']['firstTradingDate'] is None
        assert item['payload']['offeringIdentityVerified'] is False
        assert item['title'].endswith(': '+row['status'])
        assert 'first trading date not confirmed' in item['subtitle']
        assert item['source_url'] == ipo.SOURCE_URL
        assert item['date_basis'] == 'provider_ipo_status_date'
    assert ipo.normalize(ORIGINAL, MONTH, observed_at=NOW) == result
    assert any(i['symbol'] is None for i in result['items'])
    new_iceland = [i for i in result['items'] if 'new iceland' in i['company'].lower()]
    assert {i['payload']['status'] for i in new_iceland} == {'filed', 'withdrawn'}


def test_duplicate_and_correction_identity_do_not_create_another_same_status_item():
    row = deepcopy(ORIGINAL['ipoCalendar'][0])
    first = ipo.normalize({'ipoCalendar': [row, row]}, MONTH, observed_at=NOW)
    assert len(first['items']) == 1
    changed = {**row, 'date': '2026-10-22', 'price': '20.00'}
    second = ipo.normalize({'ipoCalendar': [changed]}, MONTH, observed_at=NOW)
    assert first['items'][0]['id'] == second['items'][0]['id']
    with pytest.raises(FinnhubUnavailable, match='conflicting_ipo_status_identity'):
        ipo.normalize({'ipoCalendar': [row, changed]}, MONTH, observed_at=NOW)


@pytest.mark.parametrize('change', [
    {'date': '2026-11-01'}, {'date': '20261016'}, {'date': None}, {'status': 'confirmed'},
    {'status': None}, {'name': ''}, {'name': 'hidden\nname'}, {'symbol': 'abc'},
    {'symbol': ['ABC']}, {'price': 2}, {'numberOfShares': True}, {'numberOfShares': -1},
    {'numberOfShares': float('inf')}, {'totalSharesValue': float('nan')}, {'exchange': {}},
])
def test_invalid_source_cannot_be_projected(change):
    raw = {'ipoCalendar': [{**ORIGINAL['ipoCalendar'][0], **change}]}
    with pytest.raises(FinnhubUnavailable):
        ipo.normalize(raw, MONTH, observed_at=NOW)


@pytest.mark.parametrize('raw', [None, [], {}, {'error': 'denied'}, {'ipoCalendar': None},
    {'ipoCalendar': [None]}, {'ipoCalendar': [], 'error': 'partial'}, {'ipoCalendar': [{}]*1000}])
def test_invalid_or_bounded_response_is_not_empty_success(raw):
    with pytest.raises(FinnhubUnavailable):
        ipo.normalize(raw, MONTH, observed_at=NOW)


def test_valid_empty_and_zero_are_not_missing():
    assert ipo.normalize({'ipoCalendar': []}, MONTH, observed_at=NOW)['status'] == 'empty'
    raw = {'ipoCalendar': [{**ORIGINAL['ipoCalendar'][0], 'numberOfShares': 0, 'totalSharesValue': 0}]}
    assert ipo.normalize(raw, MONTH, observed_at=NOW)['items'][0]['payload']['totalSharesValue'] == 0


@pytest.mark.parametrize('month', ['', '2026-00', '2026-13', '2026-1', '0000-01', None])
def test_invalid_month_is_rejected(month):
    with pytest.raises(FinnhubUnavailable):
        ipo.window(month)


def test_refresh_cache_repeat_and_failed_refresh_preserve_original(db, monkeypatch):
    calls = []
    def fetch(path, params):
        calls.append((path, params))
        return deepcopy(ORIGINAL)
    monkeypatch.setattr(ipo.transport, 'request_json', fetch)
    assert ipo.refresh(db, MONTH, observed_at=NOW) == {'status': 'ok', 'items': 32}
    db.commit()
    assert ipo.refresh(db, MONTH, observed_at=NOW+timedelta(hours=1))['status'] == 'cached'
    assert calls == [('calendar/ipo', {'from': '2026-10-01', 'to': '2026-10-31'})]
    row = db.get(InsightsSnapshot, ipo.key(MONTH))
    before = row.payload_json, row.fetched_at, row.source
    monkeypatch.setattr(ipo.transport, 'request_json', lambda *a: {'error': 'failed'})
    with pytest.raises(FinnhubUnavailable):
        ipo.refresh(db, MONTH, observed_at=NOW+timedelta(days=1))
    db.rollback()
    assert (row.payload_json, row.fetched_at, row.source) == before
    assert db.scalar(select(Event.id).limit(1)) is None


@pytest.mark.parametrize('mutation', ['raw', 'projection', 'version', 'malformed', 'stale', 'future'])
def test_unverified_or_stale_cache_is_never_served(db, mutation):
    row, normalized = save(db)
    if mutation in ('raw', 'projection'):
        data = json.loads(row.payload_json)
        if mutation == 'raw':
            data['raw']['ipoCalendar'][0]['date'] = '2026-10-20'
        else:
            data['normalized']['items'][0]['title'] = 'confirmed debut'
        row.payload_json = json.dumps(data)
    elif mutation == 'version': row.source = 'unreviewed'
    elif mutation == 'malformed': row.payload_json = '{'
    elif mutation == 'stale': row.fetched_at = NOW-timedelta(days=1)
    else: row.fetched_at = NOW+timedelta(seconds=1)
    db.commit()
    assert ipo.cached(db, MONTH, observed_at=NOW) is None


def test_preparation_requires_explicit_enablement_and_budget(db, monkeypatch):
    monkeypatch.setenv('CALENDAR_PROVIDER', 'fmp')
    with pytest.raises(FinnhubUnavailable, match='ipo_preparation_disabled'):
        ipo.refresh(db, MONTH)
    monkeypatch.setenv('FREE_RESEARCH_WARMING_ENABLED', '1')
    monkeypatch.setenv('FINNHUB_SHARED_LIMITER_ENABLED', '0')
    with pytest.raises(FinnhubUnavailable, match='shared_request_budget_required'):
        ipo.refresh(db, MONTH)


def test_full_original_calendar_and_digest_replay_no_network_send_or_duplicates(db):
    _, normalized = save(db)
    result = free_calendar.calendar_for_symbols(db, ['NVDA'], start=date(2026,10,1),
        end=date(2026,10,31), scope='watchlist', enqueue=False)
    assert result.items == normalized['items']  # Legacy IPO scope is global, even with a watchlist.
    assert not [e for e in result.errors if e['kind'] == 'ipo']
    assert len({i['id'] for i in result.items}) == 32
    user = _user(db, 'ipo-replay@example.test')
    _watchlist(db, user)
    db.add(NotificationSubscription(email=user.email, source_type='event_calendar', source_id='watchlist', source_name='IPO calendar',
        active=True, frequency='daily', only_if_new=False, source_payload_json=json.dumps({'calendar_kinds': ['ipo']})))
    db.commit()
    first = digests.build_signal_alert_digest(db, user, datetime(2026,10,8,tzinfo=timezone.utc),
        window_end=datetime(2026,10,9,tzinfo=timezone.utc))
    second = digests.build_signal_alert_digest(db, user, datetime(2026,10,8,tzinfo=timezone.utc),
        window_end=datetime(2026,10,9,tzinfo=timezone.utc))
    assert first.context['upcoming_events_text'] == second.context['upcoming_events_text']
    assert 'IPO: priced' in first.context['upcoming_events_text']
    assert 'IPO: filed' in first.context['upcoming_events_text']
    assert 'first trading date not confirmed' in first.context['upcoming_events_text']
    assert db.scalar(select(Event.id).limit(1)) is None


def test_off_switch_keeps_ipo_unavailable_and_never_queues_it(db, monkeypatch):
    save(db)
    monkeypatch.setenv('IPO_CALENDAR_PROVIDER', 'disabled')
    calls = []
    monkeypatch.setattr(data_enrichment_queue, 'enqueue_data_enrichment_job', lambda **kw: calls.append(kw))
    result = free_calendar.calendar_for_symbols(db, [], start=NOW.date(), end=NOW.date(), scope='all', enqueue=True)
    assert not [i for i in result.items if i['kind'] == 'ipo']
    assert {'kind': 'ipo', 'reason': 'coverage_unavailable'} in result.errors
    assert not [c for c in calls if c['payload']['dataset'] == 'ipo']


def test_cross_month_ambiguous_offering_is_held(db):
    raw = {'ipoCalendar': [ORIGINAL['ipoCalendar'][0]]}
    save(db, raw)
    save(db, {'ipoCalendar': [{**raw['ipoCalendar'][0], 'date': '2026-11-02'}]}, '2026-11')
    result = free_calendar.calendar_for_symbols(db, [], start=date(2026,10,1), end=date(2026,11,4), scope='all', enqueue=False)
    assert not [i for i in result.items if i['kind'] == 'ipo']
    assert {'kind': 'ipo', 'reason': 'conflicting_ipo_scope'} in result.errors


@pytest.mark.parametrize('observed', [datetime(2026,10,16), '2026-10-16', False, 0, ''])
def test_observation_time_must_be_an_aware_datetime(observed):
    with pytest.raises(FinnhubUnavailable, match='invalid_ipo_observation_time'):
        ipo.normalize(ORIGINAL, MONTH, observed_at=observed)


@pytest.mark.parametrize('tier,active', [('free', True), ('premium', False)])
def test_ipo_calendar_preserves_entitlement_and_optout(db, monkeypatch, tier, active):
    save(db)
    user = _user(db, 'ipo-access@example.test', tier=tier)
    db.add(NotificationSubscription(email=user.email, source_type='event_calendar', source_id='watchlist',
        source_name='IPO', active=active, frequency='daily', only_if_new=False,
        source_payload_json=json.dumps({'calendar_kinds': ['ipo']})))
    db.commit()
    monkeypatch.setattr(digests, 'upcoming_event_calendar_items', lambda *a, **k: pytest.fail('Gated calendar fetched'))
    result = digests._upcoming_calendar_events_for_digest(db, user, window_end=NOW)
    assert result[0] == [] and result[2] == ''


def test_selected_cache_miss_queues_existing_job_only(db, monkeypatch):
    monkeypatch.setenv('FINNHUB_API_KEY', 'test-only')
    calls = []
    monkeypatch.setattr(data_enrichment_queue, 'enqueue_data_enrichment_job', lambda **kw: calls.append(kw))
    result = free_calendar.calendar_for_symbols(db, [], start=NOW.date(), end=NOW.date(), scope='all', enqueue=True)
    assert {'kind': 'ipo', 'reason': 'cache_miss'} in result.errors
    matching = [c for c in calls if c['payload']['dataset'] == 'ipo']
    assert len(matching) == 1
    assert matching[0]['job_type'] == 'free_calendar'
    assert matching[0]['window_key'] == ipo.key(MONTH)


def test_refresh_rejects_remote_month_without_request(db):
    with pytest.raises(FinnhubUnavailable, match='ipo_month_outside_refresh_window'):
        ipo.refresh(db, '2020-01', observed_at=NOW)


def test_ipo_uses_existing_free_transport_header_auth_and_budget(monkeypatch):
    from app.services import finnhub_budget
    calls = []
    monkeypatch.setenv('FINNHUB_API_KEY', 'fixture-key')
    monkeypatch.setenv('FINNHUB_PAID_PREPARATION_ENABLED', '0')
    monkeypatch.setattr(finnhub_budget, 'reserve', lambda: calls.append('budget'))
    class Response:
        status_code = 200
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def iter_content(self, *args): yield json.dumps(ORIGINAL).encode()
    def get(url, **kwargs):
        calls.append('http')
        assert url.endswith('/calendar/ipo') and 'fixture-key' not in url
        assert kwargs['headers'] == {'X-Finnhub-Token': 'fixture-key'}
        assert kwargs['params'] == {'from': '2026-10-01', 'to': '2026-10-31'}
        assert kwargs['allow_redirects'] is False
        return Response()
    monkeypatch.setattr(ipo.transport.requests, 'get', get)
    assert ipo.transport.request_json('calendar/ipo', {'from': '2026-10-01', 'to': '2026-10-31'}) == ORIGINAL
    assert calls == ['budget', 'http']


def test_ipo_refresh_dispatch_uses_existing_calendar_queue_contract(monkeypatch):
    calls = []
    monkeypatch.setattr(ipo, 'refresh', lambda db, month: calls.append((db, month)) or {'status': 'ok'})
    assert free_calendar.refresh(None, 'ipo', MONTH) == {'status': 'ok'}
    assert calls == [(None, MONTH)]
