from copy import deepcopy
from datetime import date, datetime
import hashlib
import json

import pytest
from sqlalchemy import create_engine, select, func
from sqlalchemy.orm import Session

from app.db import Base
from app.models import Event, InsiderTransaction, InsiderTransactionNormalized, InstitutionalFiling, InstitutionalPosition
from app.services.direct_feed_collection import parse_document
from app.services.direct_feed_rehearsal import plan_corrections, apply_rehearsal, snapshot, include_event_corrections
from test_official_disclosure_pipelines import FORM4_SAMPLE
from test_direct_sec_collection import submission, META


@pytest.fixture
def db():
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        yield session
    engine.dispose()


def form4_document(*, repeat=False):
    xml = FORM4_SAMPLE.replace('<ownershipDocument>', '<ownershipDocument><documentType>4</documentType>')
    if repeat:
        start = xml.index('<nonDerivativeTransaction>')
        end = xml.index('</nonDerivativeTransaction>') + len('</nonDerivativeTransaction>')
        xml = xml[:end] + xml[start:end] + xml[end:]
    raw = ('<SEC-HEADER>\nACCESSION NUMBER: 0000320193-26-000001\nFILED AS OF DATE: 20260603\n</SEC-HEADER>\n' + xml).encode()
    metadata = {'key': '0000320193-26-000001', 'filing_date': '2026-06-03',
                'cik': '0000320193', 'form': '4', 'url': 'https://www.sec.gov/fixture'}
    return {'feed': 'sec_form4', 'metadata': metadata, 'raw': raw, 'content_hash': hashlib.sha256(raw).hexdigest()}


def populate_insiders(db):
    doc = form4_document()
    _, parsed, _ = parse_document(doc['feed'], doc['raw'], doc['metadata'])
    rows = []
    for i, item in enumerate(parsed['transactions']):
        row = InsiderTransactionNormalized(**{**item, 'price': 999, 'value': 999,
                                              'normalized_hash': f'legacy-{i}'})
        db.add(row)
        rows.append(row)
    db.commit()
    return doc, rows


def test_rehearsal_preserves_ids_hashes_and_repeated_apply_is_noop(db):
    doc, rows = populate_insiders(db)
    identity = [(row.id, row.normalized_hash) for row in rows]
    plan = plan_corrections(db, [doc])
    assert len(plan['operations']) == 3
    assert apply_rehearsal(db, plan)['updated'] == 3
    db.commit()
    assert apply_rehearsal(db, plan)['skipped'] == 3
    assert [(row.id, row.normalized_hash) for row in rows] == identity
    assert plan_corrections(db, [doc])['operations'] == []
    assert db.scalar(select(func.count()).select_from(InsiderTransactionNormalized)) == 3


def test_stale_row_aborts_all_updates(db):
    doc, rows = populate_insiders(db)
    plan = plan_corrections(db, [doc])
    rows[-1].ownership_nature = 'Changed after planning'
    db.commit()
    with pytest.raises(ValueError, match='changed since planning'):
        apply_rehearsal(db, plan)
    assert rows[0].price == 999


def test_new_candidate_after_plan_aborts_instead_of_duplicating(db):
    doc, rows = populate_insiders(db)
    plan = plan_corrections(db, [doc])
    copied = {column.name: getattr(rows[0], column.name) for column in rows[0].__table__.columns
              if column.name not in {'id', 'created_at', 'updated_at'}}
    db.add(InsiderTransactionNormalized(**{**copied, 'normalized_hash': 'new-duplicate'}))
    db.commit()
    with pytest.raises(ValueError, match='population changed'):
        apply_rehearsal(db, plan)
    assert rows[0].price == 999


def test_identical_source_lots_cannot_both_change_one_existing_record(db):
    _, rows = populate_insiders(db)
    plan = plan_corrections(db, [form4_document(repeat=True)])
    assert rows[0].id not in [op['id'] for op in plan['operations']]
    assert len([item for item in plan['held'] if item.get('row') in {1, 2}]) == 2


def test_tampered_source_and_plan_rejected(db):
    doc, _ = populate_insiders(db)
    with pytest.raises(ValueError, match='hash changed'):
        plan_corrections(db, [{**doc, 'raw': doc['raw'] + b'changed'}])
    plan = plan_corrections(db, [doc])
    plan['operations'][0]['changes']['price'] = 500
    with pytest.raises(ValueError, match='Plan hash changed'):
        apply_rehearsal(db, plan)


def test_rehearsal_rejects_persistent_database(tmp_path):
    engine = create_engine('sqlite:///' + (tmp_path / 'data.sqlite').as_posix())
    with Session(engine) as db:
        with pytest.raises(ValueError, match='in-memory'):
            apply_rehearsal(db, {})
    engine.dispose()


def test_13f_unit_correction_is_source_bound_and_filing_guarded(db):
    filing = InstitutionalFiling(cik=META['cik'], accession_number=META['key'], filing_date=date(2026,10,6),
                                report_year=2026, report_quarter=3, report_period_end=date(2026,9,30), form_type='13F-HR')
    db.add(filing)
    db.flush()
    position = InstitutionalPosition(filing_id=filing.id, cik=filing.cik, cusip='000361105',
                                    normalized_symbol='EXM', shares=30, value_usd=0.3,
                                    report_year=2026, report_quarter=3, filing_date=filing.filing_date)
    db.add(position)
    db.commit()
    raw = submission()
    doc = {'feed': 'sec_13f', 'metadata': META, 'raw': raw, 'content_hash': hashlib.sha256(raw).hexdigest()}
    plan = plan_corrections(db, [doc])
    assert len(plan['operations']) == 1
    assert plan['operations'][0]['source_rows'] == ['1', '2']
    assert apply_rehearsal(db, plan)['updated'] == 1
    db.commit()
    assert position.value_usd == 300 and position.normalized_symbol == 'EXM'
    assert apply_rehearsal(db, plan)['updated'] == 0
    filing.is_amendment = True
    db.commit()
    with pytest.raises(ValueError, match='state changed'):
        apply_rehearsal(db, plan)


def test_invalid_13f_cover_remains_held(db):
    raw = submission().replace(b'<tableEntryTotal>2', b'<tableEntryTotal>3')
    doc = {'feed': 'sec_13f', 'metadata': META, 'raw': raw, 'content_hash': hashlib.sha256(raw).hexdigest()}
    plan = plan_corrections(db, [doc])
    assert not plan['operations']
    assert 'count/value' in plan['held'][0]['reason']


def linked_events(db):
    from app.backfill_legacy_insider_normalized import _build_normalized_payload
    doc, rows = populate_insiders(db)
    raw_rows, events = [], []
    for row in rows:
        raw = InsiderTransaction(source='fmp', external_id=f'provider-{row.id}', symbol=row.ticker_normalized,
                                 transaction_date=row.transaction_date, filing_date=row.filing_date,
                                 transaction_type=row.transaction_code, price=row.price, shares=row.shares,
                                 payload_json=json.dumps({'accession_number': row.accession_number}))
        db.add(raw)
        db.flush()
        row.normalized_hash = _build_normalized_payload(raw)[1]['normalized_hash']
        payload = {'external_id': raw.external_id, 'transaction_date': str(row.transaction_date),
                   'filing_date': str(row.filing_date), 'raw': {'original': 999}, 'is_market_trade': True}
        event = Event(event_type='insider_trade', source='fmp', symbol=row.ticker_normalized,
                      ts=datetime(2026, 6, 1), event_date=datetime(2026, 6, 3), payload_json=json.dumps(payload))
        db.add(event)
        raw_rows.append(raw)
        events.append(event)
    db.commit()
    return doc, rows, raw_rows, events


def test_events_follow_provider_identity_preserve_dates_and_repeat_without_duplicates(db):
    from app.services.feed_pnl_enrichment import feed_pnl_inputs_for_event
    doc, rows, raw_rows, events = linked_events(db)
    originals = [snapshot(row) for row in raw_rows]
    dates = [(e.id, e.ts, e.event_date) for e in events]
    plan = include_event_corrections(db, plan_corrections(db, [doc]))
    assert plan['counts']['event_corrections'] == 3
    assert apply_rehearsal(db, plan)['updated'] == 6
    db.commit()
    assert apply_rehearsal(db, plan)['updated'] == 0
    assert [(e.id, e.ts, e.event_date) for e in events] == dates
    assert [snapshot(row) for row in raw_rows] == originals
    assert db.scalar(select(func.count()).select_from(Event)) == 3
    for row, event in zip(rows, events):
        payload = json.loads(event.payload_json)
        assert payload['raw'] == {'original': 999}
        assert payload['price'] == row.price
        assert payload['sec_verification']['sha256'] == doc['content_hash']
        if row.is_derivative:
            assert payload['is_market_trade'] is False
            assert feed_pnl_inputs_for_event(event).structural_status == 'insider_non_market'


@pytest.mark.parametrize('conflict', ['duplicate_event', 'wrong_date', 'missing_raw'])
def test_ambiguous_event_mapping_is_held(db, conflict):
    doc, rows, raw_rows, events = linked_events(db)
    if conflict == 'duplicate_event':
        copied = {column.name: getattr(events[0], column.name) for column in Event.__table__.columns
                  if column.name not in {'id', 'created_at'}}
        db.add(Event(**copied))
    elif conflict == 'wrong_date':
        payload = json.loads(events[0].payload_json)
        payload['filing_date'] = '2026-01-01'
        events[0].payload_json = json.dumps(payload)
    else:
        db.delete(raw_rows[0])
    db.commit()
    plan = include_event_corrections(db, plan_corrections(db, [doc]))
    assert plan['counts']['event_corrections'] == 2
    assert events[0].id not in [op['id'] for op in plan['operations'] if op['table'] == 'events']
    assert any('identity' in held['reason'] for held in plan['held'])


def test_raw_provider_change_invalidates_entire_event_plan(db):
    doc, rows, raw_rows, events = linked_events(db)
    plan = include_event_corrections(db, plan_corrections(db, [doc]))
    raw_rows[-1].price = 123
    db.commit()
    with pytest.raises(ValueError, match='state changed'):
        apply_rehearsal(db, plan)
    assert rows[0].price == 999
    assert events[0].source_provider is None
