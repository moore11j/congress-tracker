from datetime import date, datetime, timedelta, timezone
import pytest
from app.services import free_calendar, finnhub_free_data
from app.clients.direct_sources import DirectSourceError
from app.services.finnhub_research import FinnhubUnavailable
NOW = datetime.now(timezone.utc)

def ical(events):
    return ('BEGIN:VCALENDAR\r\n' + '\r\n'.join(events) + '\r\nEND:VCALENDAR').encode()

def event(uid='one', day='20261015', sequence=1, status='CONFIRMED', title='Consumer Price Index'):
    return f'BEGIN:VEVENT\r\nUID:{uid}\r\nSEQUENCE:{sequence}\r\nDTSTART;TZID=US-Eastern:{day}T083000\r\nSUMMARY:{title}\r\nSTATUS:{status}\r\nEND:VEVENT'

def test_official_calendar_timezone_revision_cancel_and_unfold():
    raw = ical([event(), event(day='20261016', sequence=2), event('two', day='20261201'), event('cancel'), event('cancel', sequence=2, status='CANCELLED')])
    rows = free_calendar.parse_bls(raw)
    assert len(rows) == 2
    assert rows[0]['date'] == '2026-10-16' and rows[0]['datetime'].endswith('12:30:00+00:00')
    assert rows[1]['datetime'].endswith('13:30:00+00:00')
    assert rows[0]['payload']['estimate'] is None
    assert free_calendar.parse_bls(ical([event(title='Consumer\r\n Price Index')]))[0]['title'] == 'ConsumerPrice Index'
    with pytest.raises(DirectSourceError, match='conflicting'):
        free_calendar.parse_bls(ical([event(), event(day='20261016')]))
    with pytest.raises(DirectSourceError, match='invalid_bls'):
        free_calendar.parse_bls(ical([event().replace('TZID=US-Eastern', 'TZID=Unknown')]))

def test_earnings_adjusted_estimates_date_identity_no_invented_time():
    row = {'symbol': 'ABC', 'date': '2026-10-15', 'quarter': 3, 'year': 2026, 'hour': 'amc', 'epsEstimate': -0.2, 'revenueEstimate': 12000000}
    args = dict(start=date(2026, 10, 1), end=date(2026, 10, 31), observed_at=NOW)
    rows = finnhub_free_data.earnings_calendar({'earningsCalendar': [row, row]}, **args)
    assert len(rows) == 1 and rows[0]['datetime'] is None
    assert rows[0]['payload']['accounting_basis'] == 'adjusted_non_gaap'
    assert rows[0]['payload']['epsActual'] is None
    with pytest.raises(FinnhubUnavailable, match='conflicting'):
        finnhub_free_data.earnings_calendar({'earningsCalendar': [row, {**row, 'date': '2026-10-16'}]}, **args)
    with pytest.raises(FinnhubUnavailable, match='numeric'):
        finnhub_free_data.earnings_calendar({'earningsCalendar': [{**row, 'epsEstimate': float('nan')}]}, **args)

def test_busy_earnings_month_preserves_all_rows_with_a_hard_bound():
    args = dict(start=date(2026, 10, 1), end=date(2026, 10, 31), observed_at=NOW)
    rows = [{'symbol': f'ABC{i}', 'date': '2026-10-15', 'quarter': 3, 'year': 2026}
            for i in range(1500)]
    result = finnhub_free_data.earnings_calendar({'earningsCalendar': rows}, **args)
    assert len(result) == 1500
    assert len({item['id'] for item in result}) == 1500
    with pytest.raises(FinnhubUnavailable, match='response_too_large'):
        finnhub_free_data.earnings_calendar({'earningsCalendar': rows * 7}, **args)

def test_earnings_fetch_covers_month_in_nonoverlapping_bounded_windows(monkeypatch):
    calls = []
    def fetch(path, params):
        calls.append(params)
        return {'earningsCalendar': [{'symbol': f'ABC{len(calls)}', 'date': params['from'], 'quarter': 3, 'year': 2026}]}
    monkeypatch.setattr(finnhub_free_data, 'request_json', fetch)
    rows = finnhub_free_data.fetch_earnings_calendar(date(2026,10,1), date(2026,10,31))
    assert len(rows) == len(calls) == 5
    assert calls[0]['from'] == '2026-10-01' and calls[-1]['to'] == '2026-10-31'
    for prior, following in zip(calls, calls[1:]):
        assert date.fromisoformat(prior['to']) + timedelta(days=1) == date.fromisoformat(following['from'])
    assert all((date.fromisoformat(call['to']) - date.fromisoformat(call['from'])).days <= 6 for call in calls)
    monkeypatch.setattr(finnhub_free_data, 'request_json', lambda path, params: {'earningsCalendar': [
        {'symbol': 'ABC', 'date': params['from'], 'quarter': 3, 'year': 2026}] * 1500})
    with pytest.raises(FinnhubUnavailable, match='possibly_truncated'):
        finnhub_free_data.fetch_earnings_calendar(date(2026,10,1), date(2026,10,31))


def test_saturated_week_is_split_without_publishing_the_truncated_parent(monkeypatch):
    calls = []
    def fetch(path, params):
        calls.append((params['from'], params['to']))
        start, end = date.fromisoformat(params['from']), date.fromisoformat(params['to'])
        count = 1500 if (end-start).days == 6 else 800
        return {'earningsCalendar': [{'symbol': f'ABC{start.day}X{i}', 'date': str(start),
            'quarter': 3, 'year': 2026} for i in range(count)]}
    monkeypatch.setattr(finnhub_free_data, 'request_json', fetch)
    rows = finnhub_free_data.fetch_earnings_calendar(date(2026,11,1),date(2026,11,7))
    assert len(rows) == 1600 and len({r['id'] for r in rows}) == 1600
    assert calls == [('2026-11-01','2026-11-07'),('2026-11-01','2026-11-04'),('2026-11-05','2026-11-07')]
