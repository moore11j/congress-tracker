import json
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import select
from app.models import Event, FundamentalsCache, InsightsSnapshot
from app.jobs import warm_sec_fundamentals as job
from app.services import sec_fundamentals_preparation as prep, sec_directory
from app.clients.direct_sources import DirectSourceError
from test_sec_fundamentals import sources
from test_sec_fundamentals_warming import factory


@pytest.fixture
def cached_priority(factory, monkeypatch):
    base = datetime.now(timezone.utc)
    clock = [base - timedelta(hours=18)]
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return clock[0] if tz else clock[0].replace(tzinfo=None)
    for module in (job, prep):
        monkeypatch.setattr(module, 'datetime', Clock)
    monkeypatch.setenv('SEC_FUNDAMENTALS_UNIVERSE_WARMING_ENABLED', '1')
    monkeypatch.setenv('SEC_FUNDAMENTALS_WARMING_BATCH_SIZE', '1')
    facts, company = sources()
    calls = []
    def get(client, url):
        calls.append(url)
        if url == sec_directory.URL:
            data = {'fields': ['cik', 'name', 'ticker', 'exchange'], 'data': [[1, 'Example', 'ABC', 'NYSE']]}
        else:
            data = facts if 'companyfacts' in url else company
        return json.dumps(data).encode()
    monkeypatch.setattr(sec_directory.DirectSourceClient, 'get', get)
    old = prep.prepare('ABC')
    with factory() as db:
        db.add(FundamentalsCache(symbol='AAA', provider='fmp', status='ok', fetched_at=base))
        db.commit()
    calls.clear()
    clock[0] = base
    return factory, base, clock, calls, old


def test_cache_hit_keeps_refresh_deadline_and_universe_moves(cached_priority):
    factory, base, clock, calls, old = cached_priority
    first = job.run()
    assert first['results'][0]['observed_at'] == old['observed_at']
    assert calls == []
    clock[0] = base + timedelta(minutes=1)
    assert job.run()['results'][0]['scope'] == 'AAA'
    calls.clear()
    clock[0] = base + timedelta(hours=2, seconds=1)
    due = job.run()
    assert due['results'][0]['scope'] == 'ABC'
    assert due['results'][0]['observed_at'] == clock[0].isoformat()
    assert any('companyfacts' in url for url in calls)
    assert due['canonical_writes'] == due['emails'] == 0
    with factory() as db:
        assert not list(db.scalars(select(Event)))
        assert set(db.scalars(select(FundamentalsCache.provider))) == {'fmp'}


def test_failed_refresh_preserves_evidence_and_backs_off(cached_priority, monkeypatch):
    factory, base, clock, calls, old = cached_priority
    clock[0] = base + timedelta(hours=3)
    attempted = clock[0]
    with factory() as db:
        before = db.get(InsightsSnapshot, 'sec-current-fundamentals:ABC:v1').payload_json
    def denied(client, url):
        calls.append(url)
        raise DirectSourceError('Source HTTP 429: ' + url)
    monkeypatch.setattr(sec_directory.DirectSourceClient, 'get', denied)
    first = job.run()
    assert first['results'][0]['scope'] == 'ABC'
    assert first['results'][0]['reason'].startswith('Source HTTP 429:')
    assert len(calls) == 1
    clock[0] += timedelta(minutes=5)
    assert job.run()['results'][0]['scope'] == 'AAA'
    with factory() as db:
        assert db.get(InsightsSnapshot, 'sec-current-fundamentals:ABC:v1').payload_json == before
        state = json.loads(db.get(InsightsSnapshot, job.KEY).payload_json)
    assert datetime.fromisoformat(state['priority_refresh_after']['ABC']) == attempted + timedelta(hours=20)


@pytest.mark.parametrize('age', [timedelta(0), timedelta(seconds=-1), timedelta(hours=25), '20'])
def test_invalid_cache_age_rejected_before_io(monkeypatch, age):
    monkeypatch.setenv('SEC_FUNDAMENTALS_WARMING_ENABLED', '1')
    monkeypatch.setattr(prep, 'SessionLocal', lambda: pytest.fail('database opened'))
    with pytest.raises(ValueError, match='cache age'):
        prep.prepare('ABC', max_cache_age=age)


def test_default_cache_age_remains_24_hours(cached_priority):
    factory, base, clock, calls, old = cached_priority
    clock[0] = base + timedelta(hours=3)
    assert prep.prepare('ABC') == old
    assert calls == []
    clock[0] = base + timedelta(hours=6)
    fresh = prep.prepare('ABC')
    assert fresh['observed_at'] == clock[0].isoformat()
    assert any('companyfacts' in url for url in calls)


@pytest.mark.parametrize('observation', ['invalid', '2026-10-11T00:00:00', None])
def test_invalid_observation_cannot_postpone_refresh(cached_priority, observation):
    factory, base, clock, calls, old = cached_priority
    with factory() as db:
        row = db.get(InsightsSnapshot, 'sec-current-fundamentals:ABC:v1')
        payload = json.loads(row.payload_json)
        if observation is None:
            payload.pop('observed_at')
        else:
            payload['observed_at'] = observation
        row.payload_json = json.dumps(payload)
        db.commit()
    result = job.run()
    assert result['results'][0]['observed_at'] == base.isoformat()
    assert any('companyfacts' in url for url in calls)



def test_existing_attempt_state_does_not_hide_an_already_due_cache(cached_priority):
    factory, base, clock, calls, old = cached_priority
    clock[0] = base + timedelta(hours=3)
    with factory() as db:
        db.add(InsightsSnapshot(kind=job.KEY, source='free_direct', fetched_at=base,
            payload_json=json.dumps({'attempted_at': {'ABC': base.isoformat()},
                'attempted_versions': {'ABC': prep.version_for_symbol('ABC')}})))
        db.commit()
    first = job.run()
    assert first['results'][0]['scope'] == 'ABC'
    assert first['results'][0]['observed_at'] == clock[0].isoformat()
    assert any('companyfacts' in url for url in calls)
    clock[0] += timedelta(minutes=5)
    assert job.run()['results'][0]['scope'] == 'AAA'
