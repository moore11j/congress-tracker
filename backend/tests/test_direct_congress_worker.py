from datetime import date
import json
from pathlib import Path

import pytest
from sqlalchemy import create_engine, select, func, event as sql_event
from sqlalchemy.orm import Session

from app.db import Base
from app.models import Event, Filing, Member, Security, Transaction, DataEnrichmentJob, EmailDelivery
from app.services import direct_congress_worker as worker
from app.services.direct_feed_store import discover, record_document, DirectFeedRevision, DirectFeedDocument
from app.services.direct_feed_collection import parse_document
from app.services.feed_source_control import select_feed_source, FeedSourceMismatch
from app.services.direct_congress_repair import CongressRowBinding
from test_direct_congress_repair import load_rows

FIXTURES = Path(__file__).with_name('fixtures')
DIRECTORY = [{'id': {'bioguide': 'W000802'}, 'name': {'first': 'Sheldon', 'last': 'Whitehouse'},
    'terms': [{'type': 'sen', 'state': 'RI', 'party': 'Democrat', 'start': '2025-01-03', 'end': '2031-01-03'}]}]


@pytest.fixture
def db(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail('Worker attempted network or mail transport')
    monkeypatch.setattr('requests.sessions.Session.request', forbidden)
    monkeypatch.setattr('httpx.Client.send', forbidden)
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    with Session(engine, autoflush=False) as session:
        yield session
    engine.dispose()


def stage(db, raw=None):
    case = json.loads((FIXTURES / 'congress_whitehouse_reconciliation.json').read_text())
    metadata = case['parsed']['metadata']
    raw = raw or (FIXTURES / 'senate_whitehouse_2026_10_01.html').read_bytes()
    doc = discover(db, 'senate_ptr', metadata)
    text, parsed, reasons = parse_document('senate_ptr', raw, metadata)
    assert not reasons
    record_document(db, doc, raw, text, parsed)
    db.commit()
    return doc.id, case


def activate(db, provider='official_senate', generation=0):
    select_feed_source(db, feed='senate_ptr', provider=provider, publish_since=date(2026, 9, 1),
        expected_generation=generation, reason='isolated publication test')
    db.commit()


def counts(db):
    return [db.scalar(select(func.count()).select_from(m)) for m in
        (Filing, Transaction, Event, CongressRowBinding, DataEnrichmentJob, worker.DirectFeedPublication)]


def test_publish_and_repeat_real_rows_without_email(db):
    doc_id, _ = stage(db)
    activate(db)
    result = worker.publish_document(db, doc_id, directory=DIRECTORY)
    assert result['status'] == 'published' and result['inserted_events'] == result['inserted_transactions'] == 5
    before = counts(db)
    assert before[:4] == [1, 5, 5, 5] and before[4] > 0
    assert db.scalar(select(func.count()).select_from(EmailDelivery)) == 0
    for row in db.scalars(select(Event)):
        assert row.event_date.date() == date(2026, 10, 1)
        assert json.loads(row.payload_json)['trade_date'] == '2026-09-04'
        assert row.source_provider == 'official_senate'
    assert worker.publish_document(db, doc_id, directory=DIRECTORY)['inserted_events'] == 0
    assert counts(db) == before
    assert worker.publish_batch(db, feed='senate_ptr', directory=DIRECTORY)['processed'] == 0


@pytest.mark.parametrize('fault', ['queue', 'cache', 'receipt'])
def test_atomic_failure_rolls_back_every_publication_write(db, monkeypatch, fault):
    doc_id, _ = stage(db); activate(db)
    def fail(*args, **kwargs):
        raise RuntimeError('injected interruption')
    if fault == 'queue':
        monkeypatch.setattr(worker, 'enqueue_feed_pnl_enrichment_for_event', fail)
    elif fault == 'cache':
        monkeypatch.setattr(worker, 'bump_feed_events_epoch', fail)
    else:
        def before_flush(session, *args):
            if any(isinstance(r, worker.DirectFeedPublication) for r in session.new):
                fail()
        sql_event.listen(db, 'before_flush', before_flush)
    with pytest.raises(RuntimeError, match='injected'):
        worker.publish_document(db, doc_id, directory=DIRECTORY)
    assert counts(db) == [0] * 6


@pytest.mark.parametrize('change', ['raw', 'event', 'binding', 'metadata'])
def test_repeat_detects_source_and_canonical_drift(db, change):
    doc_id, _ = stage(db); activate(db)
    worker.publish_document(db, doc_id, directory=DIRECTORY)
    if change == 'raw':
        db.scalar(select(DirectFeedRevision)).source_bytes = b'changed'
    elif change == 'event':
        db.scalar(select(Event)).amount_min = 3
    elif change == 'binding':
        db.scalar(select(CongressRowBinding)).normalized_hash = 'changed'
    else:
        doc = db.get(DirectFeedDocument, doc_id)
        metadata = json.loads(doc.metadata_json); metadata['member_name'] = 'Another Member'
        doc.metadata_json = json.dumps(metadata)
    db.commit()
    if change == 'raw':
        with pytest.raises(ValueError, match='source bytes'):
            worker.publish_document(db, doc_id, directory=DIRECTORY)
    else:
        assert worker.publish_document(db, doc_id, directory=DIRECTORY)['status'] == 'held'
    assert counts(db)[:4] == [1, 5, 5, 5]


def test_direct_activation_blocks_legacy_ingestion_and_backfill(db):
    from app.ingest_senate import upsert_senate_transaction_from_row
    from app.backfill_events_from_trades import insert_missing_congress_events_from_transactions
    doc_id, case = stage(db)
    with pytest.raises(FeedSourceMismatch):
        worker.publish_document(db, doc_id, directory=DIRECTORY)
    activate(db)
    with pytest.raises(FeedSourceMismatch):
        upsert_senate_transaction_from_row(db, {})
    for name, model in [('members', Member), ('securities', Security), ('filings', Filing), ('transactions', Transaction)]:
        load_rows(db, model, case[name])
    db.commit()
    assert insert_missing_congress_events_from_transactions(db) == 0
    db.rollback()
    activate(db, 'paused', 1)
    with pytest.raises(FeedSourceMismatch):
        worker.publish_document(db, doc_id, directory=DIRECTORY)


def test_existing_filing_binds_original_ids_and_rejects_duplicates(db):
    doc_id, case = stage(db); activate(db)
    for name, model in [('members', Member), ('securities', Security), ('filings', Filing), ('transactions', Transaction), ('events', Event)]:
        load_rows(db, model, case[name])
    db.commit()
    assert worker.publish_document(db, doc_id, directory=DIRECTORY)['status'] == 'held'
    assert counts(db)[:4] == [1, 18, 15, 0]
    for row in db.scalars(select(Transaction).where(Transaction.id.not_in(range(23181, 23186)))):
        db.delete(row)
    for row in db.scalars(select(Event).where(Event.id.not_in(range(461243, 461248)))):
        db.delete(row)
    db.commit()
    result = worker.publish_document(db, doc_id, directory=DIRECTORY)
    assert result['status'] == 'existing' and result['inserted_events'] == 0
    assert result['event_ids'] == list(range(461243, 461248))
    before = counts(db)
    assert worker.publish_document(db, doc_id, directory=DIRECTORY)['status'] == 'existing'
    assert counts(db) == before


def test_separate_identical_lots_remain_separate(db):
    import re
    raw = (FIXTURES / 'senate_whitehouse_2026_10_01.html').read_text()
    first = re.search(r'<tr><td>1</td>.*?</tr>', raw).group()
    raw = raw.replace('</tbody>', first.replace('<td>1</td>', '<td>6</td>', 1) + '</tbody>')
    doc_id, _ = stage(db, raw.encode()); activate(db)
    result = worker.publish_document(db, doc_id, directory=DIRECTORY)
    assert result['inserted_events'] == result['inserted_transactions'] == 6
    assert len(set(db.scalars(select(CongressRowBinding.normalized_hash)))) == 6
    assert worker.publish_document(db, doc_id, directory=DIRECTORY)['inserted_events'] == 0


def test_unresolved_member_is_held_before_mutation(db):
    doc_id, _ = stage(db); activate(db)
    assert worker.publish_document(db, doc_id, directory=[])['status'] == 'held'
    assert counts(db)[:5] == [0] * 5


def test_non_stock_source_rows_do_not_become_stock_alerts(db):
    key = '0541be4f-4f96-4d84-8d6f-d349f218bb2a'
    metadata = {'key': key, 'filing_id': key, 'filing_date': '2026-10-07', 'member_name': 'John Fetterman',
        'filer_type': 'John Fetterman (Senator)', 'report_title': 'Periodic Transaction Report for 10/07/2026',
        'url': f'https://efdsearch.senate.gov/search/view/ptr/{key}/'}
    raw = (FIXTURES / 'senate_fetterman_2026_10_07.html').read_bytes()
    document = discover(db, 'senate_ptr', metadata)
    text, parsed, reasons = parse_document('senate_ptr', raw, metadata)
    assert not reasons
    record_document(db, document, raw, text, parsed); db.commit()
    activate(db)
    directory = [{'id': {'bioguide': 'F000479'}, 'name': {'first': 'John', 'last': 'Fetterman'},
        'terms': [{'type': 'sen', 'state': 'PA', 'party': 'Democrat', 'start': '2023-01-03', 'end': '2029-01-03'}]}]
    result = worker.publish_document(db, document.id, directory=directory)
    assert result['inserted_transactions'] == result['non_stock_rows'] == 2
    assert result['inserted_events'] == 0
    assert counts(db)[:5] == [1, 2, 0, 2, 0]
    assert all(row.event_id is None for row in db.scalars(select(CongressRowBinding)))
    assert worker.publish_document(db, document.id, directory=directory)['status'] == 'existing'


def test_unprojected_legacy_trade_under_another_url_holds_new_source(db):
    doc_id, case = stage(db); activate(db)
    for name, model in [('members', Member), ('securities', Security), ('filings', Filing), ('transactions', Transaction)]:
        load_rows(db, model, case[name])
    db.scalar(select(Filing)).document_url = 'https://example.com/legacy-missing-source'
    db.commit()
    result = worker.publish_document(db, doc_id, directory=DIRECTORY)
    assert result['status'] == 'held' and 'overlap' in result['reason']
    assert counts(db)[:4] == [1, 18, 0, 0]


def test_prior_verified_repair_bindings_are_adopted_without_recreation(db):
    from app.services.direct_congress_repair import rehearse_duplicate_repair, filing_state, _digest
    doc_id, case = stage(db)
    for name, model in [('members', Member), ('securities', Security), ('filings', Filing), ('transactions', Transaction), ('events', Event)]:
        load_rows(db, model, case[name])
    db.commit()
    document = db.get(DirectFeedDocument, doc_id)
    source = dict(feed=document.feed, metadata=json.loads(document.metadata_json), content_hash=document.content_hash,
        raw=db.scalar(select(DirectFeedRevision)).source_bytes)
    result = rehearse_duplicate_repair(db, source, DIRECTORY, expected_before_hash=_digest(filing_state(db, document.source_url)))
    assert result['status'] == 'rehearsed'; db.commit()
    activate(db)
    assert worker.publish_document(db, doc_id, directory=DIRECTORY)['status'] == 'existing'
    assert counts(db)[:5] == [1, 5, 5, 5, 0]


@pytest.mark.parametrize('chamber', ['house', 'senate'])
def test_selected_direct_source_skips_legacy_network_fetch(db, monkeypatch, chamber):
    import importlib
    module = importlib.import_module(f'app.ingest_{chamber}')
    select_feed_source(db, feed=f'{chamber}_ptr', provider=f'official_{chamber}', publish_since=date(2026, 9, 1),
        expected_generation=0, reason='isolated no-provider-request test')
    db.commit()
    monkeypatch.setattr(module, 'SessionLocal', lambda: Session(db.get_bind()))
    monkeypatch.setattr(module, 'get_congress_metadata_resolver', lambda: pytest.fail('Legacy metadata request after switch'))
    monkeypatch.setattr(module, '_fetch_page', lambda **kwargs: pytest.fail('Legacy FMP request after switch'))
    result = getattr(module, f'ingest_{chamber}')(pages=1)
    assert result['status'] == 'skipped' and result['source_ownership_skipped']
    assert result['inserted'] == 0 and result['rows_scanned'] == 0


def test_dirty_session_rejected_before_feed_lookup_autoflush(db):
    doc_id, _ = stage(db); activate(db)
    pending = Member(bioguide_id='T000001', first_name='Test', last_name='Pending', chamber='senate')
    db.add(pending)
    with pytest.raises(ValueError, match='pending changes'):
        worker.publish_document(db, doc_id, directory=DIRECTORY)
    assert pending.id is None
    db.rollback()
