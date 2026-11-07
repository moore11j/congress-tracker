from datetime import date, datetime, timezone
from pathlib import Path
import copy
import gzip
import hashlib
import json

import pytest
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session

from app.db import Base
from app.models import (ConfirmationMonitoringSnapshot, EmailDelivery, Event, Filing,
                        MonitoringAlert, Security, Transaction, Watchlist)
from app.services import direct_congress_worker as worker, monitoring_alerts
from app.services.direct_feed_collection import parse_document
from app.services.direct_feed_store import discover, record_document
from app.services.feed_source_control import select_feed_source

FIXTURES = Path(__file__).with_name('fixtures') / 'house_common_stock'
MANIFEST = json.loads((FIXTURES / 'manifest.json').read_text())
NOW = datetime(2026, 11, 7, 12, tzinfo=timezone.utc)


class Clock(datetime):
    @classmethod
    def now(cls, tz=None):
        return NOW if tz else NOW.replace(tzinfo=None)


def original(case):
    raw = gzip.decompress((FIXTURES / case['file']).read_bytes())
    assert len(raw) == case['source_bytes']
    assert hashlib.sha256(raw).hexdigest() == case['source_sha256']
    text, parsed, reasons = parse_document('house_ptr', raw, case['metadata'])
    assert not reasons and len(parsed['transactions']) == case['rows']
    return raw, text, parsed


@pytest.fixture
def db(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail('Source review must not use HTTP or email')
    monkeypatch.setattr('requests.sessions.Session.request', forbidden)
    monkeypatch.setattr('httpx.Client.send', forbidden)
    monkeypatch.setattr(worker, 'datetime', Clock)
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    with Session(engine, autoflush=False) as session:
        session.add_all(Security(**record) for record in MANIFEST['securities'])
        session.commit()
        select_feed_source(session, feed='house_ptr', provider='official_house',
                           publish_since=date(2026, 10, 1), expected_generation=0,
                           reason='isolated whole-source common-stock validation')
        session.commit()
        yield session
    engine.dispose()


@pytest.mark.parametrize('case', MANIFEST['cases'], ids=lambda c: str(c['document_id']))
def test_complete_official_filings_preserve_history_identity_and_repeat(db, monkeypatch, case):
    raw, text, parsed = original(case)
    securities_before = [worker._record(row) for row in db.scalars(select(Security).order_by(Security.id))]
    legacy = Event(event_type='congress_trade', ts=datetime(2025, 1, 2), symbol='NVDA',
                   source='fixture_legacy', payload_json='{"instrument_type":"option","source":"historical"}')
    snapshot = ConfirmationMonitoringSnapshot(user_id=999, watchlist_id=999, ticker='NVDA',
                                              score=41, observed_at=datetime(2025, 1, 2))
    db.add_all([legacy, snapshot]); db.commit()
    legacy_before, snapshot_before = worker._record(legacy), worker._record(snapshot)
    document = discover(db, 'house_ptr', case['metadata'])
    record_document(db, document, raw, text, parsed); db.commit()
    result = worker.publish_document(db, document.id, directory=MANIFEST['directory'])
    if case['held']:
        assert result['status'] == 'held'
        assert result['reason'] == 'Existing security classification conflicts'
        assert db.scalar(select(func.count()).select_from(Transaction)) == 0
        assert db.scalar(select(func.count()).select_from(Event)) == 1
    else:
        assert result['status'] == 'published'
        assert result['inserted_events'] == result['inserted_transactions'] == case['rows']
        events = list(db.scalars(select(Event).where(Event.id != legacy.id).order_by(Event.id)))
        assert len({e.source_filing_id for e in events}) == len(events)
        source_rows = {r['normalized_hash']: r for r in parsed['transactions']}
        proofs = 0
        for event in events:
            row = source_rows[event.source_filing_id]; payload = json.loads(event.payload_json)
            assert payload['asset_class'] == 'equity' and payload['instrument_type'] == 'equity'
            assert payload['source_sha256'] == case['source_sha256']
            assert payload['trade_date'] == row['transaction_date'].isoformat()
            assert event.event_date.date().isoformat() == case['metadata']['filing_date']
            assert event.ts.date() == NOW.date()
            assert payload['source_availability']['date'] == NOW.date().isoformat()
            if row['source_line_ref'] in {r['line'] for r in case['qualified']}:
                proof = payload['source_security_classification']; proofs += 1
                assert proof['basis'] == 'official_house_common_stock_v1'
                assert proof['source_line_ref'] == row['source_line_ref']
                assert proof['security_asset_class'] in ('Stock Option', 'REIT')
            else:
                assert 'source_security_classification' not in payload
        assert proofs == len(case['qualified'])
        before = [worker._record(e) for e in events]
        repeat = worker.publish_document(db, document.id, directory=MANIFEST['directory'])
        assert repeat['status'] == 'existing' and repeat['inserted_events'] == 0
        assert [worker._record(e) for e in db.scalars(select(Event).where(Event.id != legacy.id).order_by(Event.id))] == before
        watchlist = Watchlist(name='isolated-source-review', owner_user_id=999)
        db.add(watchlist); db.flush()
        monkeypatch.setattr(monitoring_alerts, '_user_can_view_signal_context', lambda *a: False)
        assert all(monitoring_alerts._ensure_alert_for_event(db, user_id=999, watchlist=watchlist, event=e) for e in events)
        assert not any(monitoring_alerts._ensure_alert_for_event(db, user_id=999, watchlist=watchlist, event=e) for e in events)
        assert db.scalar(select(func.count()).select_from(MonitoringAlert)) == len(events)
    assert [worker._record(row) for row in db.scalars(select(Security).order_by(Security.id))] == securities_before
    assert worker._record(db.get(Event, legacy.id)) == legacy_before
    assert worker._record(db.get(ConfirmationMonitoringSnapshot, snapshot.id)) == snapshot_before
    assert db.scalar(select(func.count()).select_from(EmailDelivery)) == 0


@pytest.fixture(scope='module')
def verified_stock_row():
    case = next(c for c in MANIFEST['cases'] if c['document_id'] == 11452)
    return original(case)[2]['transactions'][0]


@pytest.mark.parametrize('fault', ['senate', 'option', 'unknown_label', 'fund', 'etf', 'bond',
                                  'wrong_symbol', 'wrong_issuer', 'no_common_stock',
                                  'option_suffix', 'conflicting_name', 'wrong_parser', 'amendment'])
def test_conflicting_or_incomplete_instrument_evidence_stays_held(verified_stock_row, fault):
    row = copy.deepcopy(verified_stock_row)
    security = Security(symbol='NVDA', name='NVIDIA Corporation', asset_class='Stock Option')
    feed = 'house_ptr'
    assert worker._source_common_stock_proof(feed, row, security)
    if fault == 'senate': feed = 'senate_ptr'
    elif fault == 'option': row.update(asset_type_raw='option', asset_type_normalized='option')
    elif fault == 'unknown_label': security.asset_class = 'Unknown'
    elif fault in ('fund', 'etf', 'bond'): security.asset_class = fault
    elif fault == 'wrong_symbol': security.symbol = 'OTHER'
    elif fault == 'wrong_issuer': security.name = 'Different Corporation'
    elif fault == 'no_common_stock': row['issuer_name_raw'] = 'NVIDIA Corporation (NVDA)'
    elif fault == 'option_suffix': row['issuer_name_raw'] += ' Call Option'
    elif fault == 'conflicting_name': row['security_name_raw'] = 'Unrelated security'
    elif fault == 'wrong_parser': row['parser_version'] = 'unverified'
    elif fault == 'amendment': row['amendment_flag'] = True
    assert worker._source_common_stock_proof(feed, row, security) is None
