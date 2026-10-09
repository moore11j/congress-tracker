from datetime import date, datetime, timedelta, timezone
import json
import pytest
from app.models import Event, InsightsSnapshot
from app.services import free_calendar
from test_email_digests import _session
NOW = datetime.now(timezone.utc)

@pytest.fixture
def db(monkeypatch):
    monkeypatch.setenv('FMP_PROVIDER_DISABLED', '1')
    db = _session()
    yield db
    db.close()

def ical(events):
    return ('BEGIN:VCALENDAR\r\n' + '\r\n'.join(events) + '\r\nEND:VCALENDAR').encode()

def event(uid='one', day='20261015', sequence=1, status='CONFIRMED', title='Consumer Price Index'):
    return f'BEGIN:VEVENT\r\nUID:{uid}\r\nSEQUENCE:{sequence}\r\nDTSTART;TZID=US-Eastern:{day}T083000\r\nSUMMARY:{title}\r\nSTATUS:{status}\r\nEND:VEVENT'

def test_calendar_reads_are_cache_only_partial_and_no_send(db, monkeypatch):
    monkeypatch.setenv('CALENDAR_PROVIDER', 'free_direct')
    monkeypatch.setattr(free_calendar.DirectSourceClient, 'get', lambda *a: pytest.fail('Reader performed network fetch'))
    from app.services import data_enrichment_queue
    monkeypatch.setattr(data_enrichment_queue, 'enqueue_data_enrichment_job', lambda **kw: pytest.fail('Digest enqueued work'))
    items = free_calendar.parse_bls(ical([event(day=NOW.strftime('%Y%m%d'))]))
    db.add(InsightsSnapshot(kind='free-calendar:bls:v1', source='bls', fetched_at=NOW, payload_json=json.dumps({'source': 'bls', 'items': items})))
    db.commit()
    result = free_calendar.calendar_for_symbols(db, ['ABC'], start=NOW.date(), end=NOW.date()+timedelta(days=1), scope='watchlist', enqueue=False)
    assert len(result.items) == 1
    assert {'kind': 'economic', 'reason': 'bls_only_no_consensus'} in result.errors
    assert {'kind': 'earnings', 'reason': 'cache_miss'} in result.errors
    assert {'kind': 'economic', 'reason': 'schedule_horizon_incomplete'} in result.errors

def test_calendar_background_refresh_repeats_without_requests(db, monkeypatch):
    monkeypatch.setenv('CALENDAR_PROVIDER', 'free_direct')
    calls = []
    def fetch(*args):
        calls.append(1)
        return ical([event(day=NOW.strftime('%Y%m%d'))])
    monkeypatch.setattr(free_calendar.DirectSourceClient, 'get', fetch)
    assert free_calendar.refresh(db, 'bls', NOW.strftime('%Y-%m'))['status'] == 'ok'
    db.commit()
    assert free_calendar.refresh(db, 'bls', NOW.strftime('%Y-%m'))['status'] == 'cached'
    assert len(calls) == 1

def test_free_analyst_panel_isolated_entitled_no_fake_price_targets_or_score(db, monkeypatch):
    from app.services import replacement_analysts, analyst_consensus, data_enrichment_queue
    from app.services.finnhub_research import normalize_recommendations
    monkeypatch.setenv('ANALYST_PROVIDER', 'finnhub')
    calls = []
    def fetch(symbol, observed_at):
        calls.append(symbol)
        return normalize_recommendations([{'symbol': symbol, 'period': NOW.date().replace(day=1).isoformat(),
            'strongBuy': 2, 'buy': 3, 'hold': 4, 'sell': 1, 'strongSell': 0}], symbol, observed_at=observed_at)
    monkeypatch.setattr(replacement_analysts, 'fetch_recommendations', fetch)
    monkeypatch.setattr(analyst_consensus, 'fetch_grades_summary', lambda **kw: pytest.fail('FMP fetch'))
    replacement_analysts.refresh(db, 'ABC'); db.commit()
    replacement_analysts.refresh(db, 'ABC')
    assert calls == ['ABC']
    payload = analyst_consensus.current_consensus_payload(db, 'ABC', include_details=True)
    assert payload['currentSnapshot']['recommendationDistribution']['total'] == 10
    assert payload['currentSnapshot']['priceTargetRange']['consensus'] is None
    assert payload['currentSnapshot']['source'] == 'finnhub'
    assert payload['availability']['status'] == 'partial'
    public = analyst_consensus.current_consensus_payload(db, 'ABC', include_details=False)
    assert 'recommendationDistribution' not in public['currentSnapshot']
    assert public['access']['detailsLocked']
    assert analyst_consensus.analyst_consensus_component_inputs(db, 'ABC')['inputs']['freshnessStatus'] == 'unavailable'
    assert analyst_consensus.ingest_symbol_consensus(db, 'ABC')['error'] == 'provider_disabled'
    assert db.query(Event).count() == 0

def test_free_analyst_cache_miss_queues_only(db, monkeypatch):
    from app.services import replacement_analysts, analyst_consensus, data_enrichment_queue
    monkeypatch.setenv('ANALYST_PROVIDER', 'finnhub')
    monkeypatch.setenv('FINNHUB_API_KEY', 'fixture-only')
    monkeypatch.setattr(replacement_analysts, 'fetch_recommendations', lambda *a, **kw: pytest.fail('Public fetch'))
    calls = []
    monkeypatch.setattr(data_enrichment_queue, 'enqueue_data_enrichment_job', lambda **kw: calls.append(kw))
    assert analyst_consensus.current_consensus_payload(db, 'ABC')['currentSnapshot'] is None
    assert calls[0]['job_type'] == 'analyst_recommendations'
