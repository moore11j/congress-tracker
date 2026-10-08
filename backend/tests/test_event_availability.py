from datetime import date, datetime, timezone
import json

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.models import Event
from app.services.event_availability import available_event_date
from app.services.backtesting.queries import event_entry_date, congress_entry_date
from app.services.backtesting.signal_mixer import load_mixer_events
from app.services.replicated_portfolios import _event_public_date


def event(payload):
    return Event(id=1, event_type='congress_trade', symbol='NOW', source='house_fmp',
        ts=datetime(2026, 10, 7, tzinfo=timezone.utc), event_date=datetime(2026, 10, 2, tzinfo=timezone.utc),
        trade_type='purchase', transaction_type='purchase', member_bioguide_id='W000829',
        payload_json=json.dumps(payload))


def test_corrected_filing_retains_every_execution_day_and_transaction_date():
    before = {'filing_date':'2026-10-07', 'report_date':'2026-10-07', 'trade_date':'2026-09-17'}
    after = {**before, 'filing_date':'2026-10-02', 'report_date':'2026-10-02',
             'source_availability': {'date':'2026-10-07', 'basis':'retained_legacy_report_date'}}
    row = event(after)
    for resolver in (event_entry_date, _event_public_date):
        assert resolver(row, before) == resolver(row, after) == date(2026, 10, 7)
    assert congress_entry_date(row, after, portfolio_model='disclosure_date') == date(2026,10,7)
    assert congress_entry_date(row, after, portfolio_model='trade_date') == date(2026,9,17)


@pytest.mark.parametrize('bad', [None, [], {}, {'date':'2026-10-01','basis':'direct_publication'},
    {'date':'invalid','basis':'direct_publication'}, {'date':'2026-10-07','basis':'unknown'},
    {'date':None,'basis':'direct_publication'}])
def test_invalid_explicit_availability_does_not_fall_back_to_old_date(bad):
    payload = {'filing_date':'2026-10-02', 'source_availability':bad}
    assert available_event_date(payload, date(2026,10,2)) is None
    assert event_entry_date(event(payload), payload) is None
    assert _event_public_date(event(payload), payload) is None


def test_legacy_missing_evidence_keeps_original_semantics():
    assert available_event_date({}, date(2026,10,2)) == date(2026,10,2)
    assert available_event_date({}, None) is None
    assert available_event_date({'source_availability': {'date':'2026-10-07',
        'basis':'direct_publication'}}, None) is None


def test_actual_mixer_window_uses_availability_without_future_leakage():
    engine = create_engine('sqlite:///:memory:')
    Event.__table__.create(engine)
    payload = {'filing_date':'2026-10-02', 'trade_date':'2026-09-17', 'transaction_type':'purchase',
        'source_availability': {'date':'2026-10-07','basis':'retained_legacy_report_date'}}
    with Session(engine) as db:
        db.add(event(payload)); db.commit()
        early, missing = load_mixer_events(db, 'congress', date(2026,10,1), date(2026,10,3))
        assert early == [] and missing == 0
        available, missing = load_mixer_events(db, 'congress', date(2026,10,7), date(2026,10,7))
        assert len(available) == 1 and missing == 0
        assert available[0].day == date(2026,10,7)
    engine.dispose()
