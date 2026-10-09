from datetime import datetime, timezone
import json

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from app.db import Base
from app.models import InsightsSnapshot
from app.jobs import warm_free_research as job
from app.services import free_calendar, replacement_analysts, data_enrichment_queue as queue
from app.services.finnhub_research import FinnhubUnavailable


@pytest.fixture
def factory(monkeypatch):
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    sessions = sessionmaker(bind=engine)
    monkeypatch.setattr(job, 'SessionLocal', sessions)
    monkeypatch.setenv('FREE_RESEARCH_WARMING_ENABLED', '1')
    monkeypatch.setenv('FINNHUB_API_KEY', 'test-only')
    monkeypatch.setenv('FINNHUB_SHARED_LIMITER_ENABLED', '1')
    monkeypatch.setenv('ANALYST_PROVIDER', 'fmp')
    monkeypatch.setenv('CALENDAR_PROVIDER', 'fmp')
    monkeypatch.setattr(queue, '_recently_viewed_ticker_symbols', lambda *a, **k: [])
    monkeypatch.setattr(queue, 'DEFAULT_PREWARM_SYMBOLS', ['ABC', 'DEF'])
    yield sessions
    engine.dispose()


def test_disabled_and_missing_budget_do_not_open_database(factory, monkeypatch):
    monkeypatch.setattr(job, 'SessionLocal', lambda: pytest.fail('database opened'))
    monkeypatch.setenv('FREE_RESEARCH_WARMING_ENABLED', '0')
    assert job.run()['status'] == 'disabled'
    monkeypatch.setenv('FREE_RESEARCH_WARMING_ENABLED', '1')
    monkeypatch.setenv('FINNHUB_SHARED_LIMITER_ENABLED', '0')
    assert job.run()['reason'] == 'shared_budget_required'


def test_shadow_prepares_real_caches_without_selecting_or_publishing(factory, monkeypatch):
    calls = []
    period = datetime.now(timezone.utc).date().replace(day=1).isoformat()
    def recommendations(symbol, observed_at):
        calls.append(symbol)
        return {'status': 'ok', 'symbol': symbol, 'source': 'finnhub', 'observed_at': observed_at.isoformat(),
                'current': {'period': period, 'buy': 1, 'hold': 0, 'sell': 0, 'strongBuy': 0, 'strongSell': 0, 'total': 1}}
    monkeypatch.setattr(replacement_analysts, 'fetch_recommendations', recommendations)
    monkeypatch.setattr(free_calendar.DirectSourceClient, 'get', lambda *a: b'official-calendar-fixture')
    monkeypatch.setattr(free_calendar, 'parse_bls', lambda raw: [])
    from app.services import finnhub_free_data
    monkeypatch.setattr(finnhub_free_data, 'fetch_earnings_calendar', lambda *a, **k: [])
    first = job.run()
    assert first['status'] == 'ok' and first['completed_scopes'] == 5
    assert first['analyst_selection'] == first['calendar_selection'] == 'fmp'
    assert first['canonical_writes'] == first['emails'] == 0
    assert not replacement_analysts.selected() and not free_calendar.selected()
    second = job.run()
    assert all(row['status'] == 'cached' for row in second['results'])
    assert calls == ['ABC', 'DEF']
    with factory() as db:
        assert len(list(db.scalars(select(InsightsSnapshot)))) == 6
        assert not json.loads(db.get(InsightsSnapshot, job.KEY).payload_json).get('lease')
        assert replacement_analysts.current_payload(db, 'ABC', include_details=True)['currentSnapshot']['source'] == 'finnhub'


def test_failures_rotate_and_active_worker_cannot_overlap(factory, monkeypatch):
    symbols = ['T'+chr(65+i) for i in range(21)]
    monkeypatch.setattr(queue, 'DEFAULT_PREWARM_SYMBOLS', symbols)
    monkeypatch.setattr(free_calendar, 'refresh', lambda *a: {'status': 'ok'})
    calls = []
    def refresh(db, symbol):
        assert job.run()['reason'] == 'active_warming_lease'
        calls.append(symbol)
        raise FinnhubUnavailable('symbol_mismatch')
    monkeypatch.setattr(replacement_analysts, 'refresh', refresh)
    assert job.run()['status'] == 'partial'
    assert calls == symbols[:20]
    calls.clear()
    job.run()
    assert calls[0] == symbols[-1]


def test_shared_cooldown_stops_before_further_endpoints(factory, monkeypatch):
    def limited(*args):
        raise FinnhubUnavailable('provider_cooldown')
    monkeypatch.setattr(free_calendar, 'refresh', limited)
    monkeypatch.setattr(replacement_analysts, 'refresh', lambda *a: pytest.fail('continued after cooldown'))
    result = job.run()
    assert result['status'] == 'partial' and result['completed_scopes'] == 1
    with factory() as db:
        assert not json.loads(db.get(InsightsSnapshot, job.KEY).payload_json).get('lease')


def test_unselected_refresh_still_requires_explicit_shadow_flag(factory, monkeypatch):
    monkeypatch.setenv('FREE_RESEARCH_WARMING_ENABLED', '0')
    with factory() as db:
        with pytest.raises(ValueError):
            replacement_analysts.refresh(db, 'ABC')
        with pytest.raises(ValueError):
            free_calendar.refresh(db, 'bls', datetime.now(timezone.utc).strftime('%Y-%m'))
