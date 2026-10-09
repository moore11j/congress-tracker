"""Real canonical 13F processing and no-send monitoring checks in isolated data."""
from datetime import date, datetime, timezone
import hashlib
import json

import pytest
from sqlalchemy import create_engine, select, func
from sqlalchemy.orm import Session

from app.db import Base
from app.models import (EmailDelivery, Event, InstitutionalActivityEvent, InstitutionalFiling,
                        InstitutionalPosition, InstitutionalPositionChange, MonitoringAlert)
from app.services.direct_13f_publication import rehearse_new_13f
from app.services import institutional_activity as activity
from app.services.monitoring_alerts import _ensure_alert_for_event
from test_email_digests import _user, _watchlist


SINCE = date(2026, 10, 1)
CIK = '0001092903'


def document(*, quarter=3, serial=1, rows=None, confidential=False, amendment=False, filed=None):
    period = '09-30-2026' if quarter == 3 else '06-30-2026'
    filed = filed or ('2026-10-06' if quarter == 3 else '2026-08-06')
    accession = f'0001096906-26-{serial:06}'
    form = '13F-HR/A' if amendment else '13F-HR'
    rows = rows or [('000361105', 10_000_000, 100_000_000, '')]
    body = ''.join(f'''<infoTable><nameOfIssuer>Example</nameOfIssuer><titleOfClass>COM</titleOfClass>
        <cusip>{cusip}</cusip><value>{value}</value><shrsOrPrnAmt><sshPrnamt>{shares}</sshPrnamt>
        <sshPrnamtType>SH</sshPrnamtType></shrsOrPrnAmt>{f'<putCall>{option}</putCall>' if option else ''}</infoTable>'''
        for cusip, shares, value, option in rows)
    raw = f'''<SEC-HEADER>ACCESSION NUMBER: {accession}
FILED AS OF DATE: {filed.replace('-', '')}</SEC-HEADER>
<DOCUMENT><FILENAME>primary.xml
<XML><edgarSubmission><headerData><submissionType>{form}</submissionType><filerInfo><filer><credentials>
<cik>{CIK}</cik></credentials></filer></filerInfo></headerData><formData><coverPage>
<reportCalendarOrQuarter>{period}</reportCalendarOrQuarter></coverPage><summaryPage>
<tableEntryTotal>{len(rows)}</tableEntryTotal><tableValueTotal>{sum(r[2] for r in rows)}</tableValueTotal>
<isConfidentialOmitted>{str(confidential).lower()}</isConfidentialOmitted></summaryPage></formData></edgarSubmission></XML></DOCUMENT>
<DOCUMENT><FILENAME>holdings.xml
<XML><informationTable>{body}</informationTable></XML></DOCUMENT>'''.encode()
    return {'feed': 'sec_13f', 'raw': raw, 'content_hash': hashlib.sha256(raw).hexdigest(),
        'metadata': {'key': accession, 'cik': CIK, 'name': 'Example Manager', 'form': form, 'filing_date': filed,
                     'url': f'https://www.sec.gov/Archives/edgar/data/{int(CIK)}/{accession}.txt'}}


@pytest.fixture
def db(monkeypatch):
    def forbidden(*a, **k):
        pytest.fail('Network or customer delivery attempted')
    monkeypatch.setattr('requests.sessions.Session.request', forbidden)
    monkeypatch.setattr('httpx.Client.send', forbidden)
    monkeypatch.setattr('app.services.email_digests._send_digest', forbidden)
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        yield session
    engine.dispose()


def mappings(db):
    # Earlier observations from a separate manager establish identifiers only.
    for i, (cusip, symbol) in enumerate([('000361105', 'AAA'), ('000361106', 'BBB'), ('000361107', 'CCC')]):
        db.add(InstitutionalPosition(filing_id=999, cik='0000000999', cusip=cusip, normalized_symbol=symbol,
            symbol=symbol, shares=1, value_usd=10, report_year=2025, report_quarter=4, filing_date=date(2026, 2, 1)))
    db.commit()


def publish(db, doc):
    result = rehearse_new_13f(db, doc, publish_since=SINCE)
    db.commit()
    return result


def count(db, model):
    return db.scalar(select(func.count()).select_from(model))


def test_first_filing_records_holdings_without_inventing_first_time_buys(db):
    mappings(db)
    doc = document(rows=[('000361105', 10, 100, ''), ('000361105', 10, 100, '')])
    result = publish(db, doc)
    assert result['inserted_positions'] == 1 and result['derived_state'] == 'waiting_complete_prior_quarter'
    filing = db.scalar(select(InstitutionalFiling))
    row = db.scalar(select(InstitutionalPosition).where(InstitutionalPosition.filing_id == filing.id))
    assert row.shares == 20 and row.value_usd == 200 and row.portfolio_weight == 100
    assert count(db, InstitutionalPositionChange) == count(db, Event) == 0
    assert publish(db, doc)['inserted_positions'] == 0


def test_verified_pair_runs_actual_changes_events_and_monitoring_without_duplicates(db, monkeypatch):
    mappings(db)
    prior = document(quarter=2, serial=2, rows=[('000361105', 10_000_000, 100_000_000, ''), ('000361106', 10_000_000, 100_000_000, '')])
    assert publish(db, prior)['derived_state'] == 'historical_no_alerts'
    current = document(rows=[('000361105', 30_000_000, 300_000_000, ''), ('000361107', 20_000_000, 200_000_000, '')])
    result = publish(db, current)
    assert result['derived_state'] == 'published' and result['changes'] == 3
    changes = {r.normalized_symbol: r for r in db.scalars(select(InstitutionalPositionChange))}
    assert changes['AAA'].change_type == 'increase' and changes['AAA'].shares_delta == 20_000_000
    assert changes['BBB'].change_type == 'exit' and changes['CCC'].change_type == 'new_position'
    events = list(db.scalars(select(Event)))
    assert events and result['feed_events'] == len(events)
    user = _user(db, '13f-rehearsal@example.test', tier='pro')
    watchlist = _watchlist(db, user)
    restricted = _user(db, '13f-restricted@example.test', tier='premium')
    for event in events:
        assert event.event_date.date() == date(2026, 10, 6)
        assert json.loads(event.payload_json)['sec_verification']['prior_accession'] == prior['metadata']['key']
        assert _ensure_alert_for_event(db, user_id=user.id, watchlist=watchlist, event=event)
        assert not _ensure_alert_for_event(db, user_id=restricted.id, watchlist=watchlist, event=event)
    db.commit()
    identities = [(e.id, e.source_filing_id, e.payload_json) for e in events]
    alert_ids = list(db.scalars(select(MonitoringAlert.id)))
    assert alert_ids and count(db, EmailDelivery) == 0
    from app.services import email_digests as digests
    from app.models import Security, WatchlistItem
    for symbol in ('AAA', 'BBB', 'CCC'):
        security = Security(symbol=symbol, name=symbol, asset_class='stock')
        db.add(security)
        db.flush()
        db.add(WatchlistItem(watchlist_id=watchlist.id, security_id=security.id))
    db.commit()
    monkeypatch.setattr(digests, '_upcoming_calendar_events_for_digest', lambda *a, **k: ([], 'Outside 13F rehearsal', None))
    since, end = datetime(2026, 10, 5, tzinfo=timezone.utc), datetime.now(timezone.utc)
    builds = lambda: [digests.build_monitoring_digest(db, user, watchlist, since, window_end=end),
                      digests.build_signal_alert_digest(db, user, since, window_end=end),
                      digests.build_watchlist_activity_digest(db, user, watchlist, since)]
    before_digests = builds()
    assert all(d.items_count > 0 for d in before_digests)
    for event in events:
        assert event.symbol in before_digests[1].context['institutional_activity_text']
    assert 'BBB' in before_digests[1].context['institutional_activity_text']
    assert '13F' in str(before_digests[2].items)
    assert before_digests[2].items[0]['date'] == 'Oct 6, 2026'
    assert before_digests[2].items[0]['trade'] == '13F Reported Exit'
    assert before_digests[2].items[0]['amount'] == 'Prior reported holding: $100,000,000'
    assert publish(db, current)['feed_events'] == 0
    assert [(e.id, e.source_filing_id, e.payload_json) for e in db.scalars(select(Event))] == identities
    for event in events:
        assert not _ensure_alert_for_event(db, user_id=user.id, watchlist=watchlist, event=event)
    db.commit()
    assert list(db.scalars(select(MonitoringAlert.id))) == alert_ids
    assert [(d.items, d.context) for d in builds()] == [(d.items, d.context) for d in before_digests]
    assert count(db, EmailDelivery) == 0


@pytest.mark.parametrize('kwargs', [{'confidential': True}, {'amendment': True}])
def test_incomplete_or_amended_filing_never_creates_a_false_exit(db, kwargs):
    mappings(db)
    publish(db, document(quarter=2, serial=2))
    result = publish(db, document(**kwargs))
    assert result['status'] == 'held' and count(db, InstitutionalFiling) == 1
    assert count(db, InstitutionalPositionChange) == count(db, Event) == 0


def test_missing_mapping_retains_holdings_but_blocks_equity_changes(db):
    publish(db, document(quarter=2, serial=2))
    result = publish(db, document())
    assert result['inserted_positions'] == 1 and result['derived_state'] == 'waiting_equity_symbol_mapping'
    assert count(db, InstitutionalPositionChange) == 0


def test_out_of_order_prior_can_unlock_current_exactly_once(db):
    mappings(db)
    current = document(rows=[('000361105', 30_000_000, 300_000_000, '')])
    assert publish(db, current)['derived_state'] == 'waiting_complete_prior_quarter'
    publish(db, document(quarter=2, serial=2))
    assert publish(db, current)['derived_state'] == 'published'
    assert publish(db, current)['feed_events'] == 0


def test_later_disclosed_prior_cannot_be_used_as_available_evidence(db):
    mappings(db)
    publish(db, document(quarter=2, serial=2, filed='2026-10-07'))
    assert publish(db, document())['derived_state'] == 'waiting_verified_prior_quarter'
    assert count(db, Event) == 0


def test_existing_holder_quarter_with_other_accession_is_held(db):
    publish(db, document())
    assert publish(db, document(serial=3))['status'] == 'held'
    assert count(db, InstitutionalFiling) == 1


def test_changed_prior_state_is_not_silently_reused(db):
    mappings(db)
    prior = document(quarter=2, serial=2)
    publish(db, prior)
    current = document(rows=[('000361105', 30_000_000, 300_000_000, '')])
    publish(db, current)
    filing = db.scalar(select(InstitutionalFiling).where(InstitutionalFiling.accession_number == prior['metadata']['key']))
    row = db.scalar(select(InstitutionalPosition).where(InstitutionalPosition.filing_id == filing.id))
    row.shares += 1
    db.commit()
    assert publish(db, current)['status'] == 'held'


def test_modified_prior_disclosure_date_cannot_establish_earlier_availability(db):
    mappings(db)
    prior = document(quarter=2, serial=2, filed='2026-10-07')
    publish(db, prior)
    filing = db.scalar(select(InstitutionalFiling))
    filing.filing_date = date(2026, 8, 6)
    db.commit()
    assert publish(db, document())['derived_state'] == 'waiting_verified_prior_quarter'
    assert count(db, Event) == 0


def test_atomic_publication_rolls_back_if_derived_builder_fails(db, monkeypatch):
    mappings(db)
    publish(db, document(quarter=2, serial=2))
    before = count(db, InstitutionalPosition)
    def fail(*a, **k):
        raise ValueError('simulated derived failure')
    monkeypatch.setattr(activity, 'process_filing_changes_and_events', fail)
    with pytest.raises(ValueError, match='simulated'):
        rehearse_new_13f(db, document(), publish_since=SINCE)
    assert count(db, InstitutionalFiling) == 1 and count(db, InstitutionalPosition) == before
    assert count(db, Event) == 0


def test_options_do_not_become_equity_share_changes(db):
    mappings(db)
    publish(db, document(quarter=2, serial=2, rows=[('000361105', 10, 100, 'PUT')]))
    result = publish(db, document(rows=[('000361105', 30, 300, 'PUT')]))
    assert result['derived_state'] == 'published' and result['changes'] == 0
    assert count(db, Event) == 0


def test_checksum_and_database_boundary_are_enforced(db):
    doc = document()
    with pytest.raises(ValueError, match='checksum'):
        rehearse_new_13f(db, {**doc, 'raw': doc['raw'] + b'changed'}, publish_since=SINCE)
    engine = create_engine('sqlite://')
    with Session(engine) as other, pytest.raises(ValueError, match='in-memory'):
        rehearse_new_13f(other, doc, publish_since=SINCE)
    engine.dispose()


def test_orphan_position_with_unpadded_cik_blocks_duplicate_publication(db):
    db.add(InstitutionalPosition(filing_id=999, cik=CIK.lstrip('0'), cusip='000361105',
        normalized_symbol='AAA', shares=10, value_usd=100, report_year=2026, report_quarter=3,
        filing_date=date(2026, 10, 6)))
    db.commit()
    assert publish(db, document())['status'] == 'held'
    assert count(db, InstitutionalFiling) == 0 and count(db, InstitutionalPosition) == 1


def test_orphan_feed_event_blocks_duplicate_publication(db):
    db.add(Event(event_type='new_institutional_position', source='13F filing', ts=datetime(2026, 10, 6),
        source_provider=activity.INSTITUTIONAL_EVENT_SOURCE, member_bioguide_id=CIK.lstrip('0'),
        payload_json=json.dumps({'report_year': 2026, 'report_quarter': 3})))
    db.commit()
    assert publish(db, document())['status'] == 'held'
    assert count(db, InstitutionalFiling) == 0 and count(db, Event) == 1


@pytest.mark.parametrize('value,expected', [(None, '--'), (0, 'Reported holding: $0'), (1250, 'Reported holding: $1,250')])
def test_reported_holdings_never_render_as_execution_prices_or_trade_amounts(value, expected):
    from app.services.email_digests import _activity_item_from_event, _event_item
    event = Event(event_type='institutional_accumulation', symbol='AAA', trade_type='Buy',
        event_date=datetime(2026, 10, 6, tzinfo=timezone.utc), amount_min=999, amount_max=999,
        payload_json=json.dumps({'data_semantics': 'institutional_13f_reported_holdings',
            'filing_date': '2026-10-06', 'reported_value_usd': value, 'trade_price': 123}))
    activity_item, watchlist_item = _activity_item_from_event(event), _event_item(event)
    assert activity_item['action'] == watchlist_item['trade'] == '13F Reported Increase'
    assert activity_item['value'] == watchlist_item['amount'] == expected
    assert activity_item['date'] == watchlist_item['date'] == 'Oct 6, 2026'
    assert activity_item['trade_price'] == '--'
