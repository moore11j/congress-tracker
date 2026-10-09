from datetime import date, datetime, timezone, timedelta
import hashlib
import json

import pytest
from sqlalchemy import create_engine, event, func, inspect, select, text
from sqlalchemy.orm import Session, sessionmaker

from app.db import Base
from app.models import AppSetting, DataEnrichmentJob, EmailDelivery, Event, InsiderTransactionNormalized, SecForm4Filing
from app.services.direct_feed_collection import parse_document
from app.services.direct_feed_store import (DirectFeedDocument, DirectFeedRevision, discover,
    ensure_direct_feed_schema, record_document)
from app.services import direct_feed_worker as worker
from app.services.feed_source_control import (FeedSourceControl, FeedSourceMismatch, FeedWriterBusy,
    lock_feed, select_feed_source, writer_transaction)
from test_direct_feed_rehearsal import form4_document


@pytest.fixture
def db(monkeypatch):
    def forbidden(*a, **k):
        pytest.fail('Publication attempted network/mail delivery')
    monkeypatch.setattr('requests.sessions.Session.request', forbidden)
    monkeypatch.setattr('httpx.Client.send', forbidden)
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        yield session
    engine.dispose()


def stage(db):
    doc = form4_document(repeat=True)
    metadata = doc['metadata']
    metadata['url'] = f'https://www.sec.gov/Archives/edgar/data/320193/{metadata["key"]}.txt'
    row = discover(db, 'sec_form4', metadata)
    source, parsed, reasons = parse_document(doc['feed'], doc['raw'], metadata)
    assert not reasons
    record_document(db, row, doc['raw'], source, parsed)
    db.commit()
    return row.id, doc


def activate(db, *, provider='sec_edgar', generation=0, since=date(2026, 6, 1)):
    select_feed_source(db, feed='sec_form4', provider=provider, publish_since=since,
                       expected_generation=generation, reason='isolated test activation')
    db.commit()


def counts(db):
    return [db.scalar(select(func.count()).select_from(model)) for model in
            (SecForm4Filing, InsiderTransactionNormalized, Event, DataEnrichmentJob, worker.DirectFeedPublication)]


def test_publish_commit_repeat_and_enrichment_without_email(db):
    document_id, doc = stage(db)
    activate(db)
    result = worker.publish_document(db, document_id)
    assert result['status'] == 'published' and result['inserted_events'] == 4
    before = counts(db)
    assert before[:3] == [1, 4, 4] and before[3] > 0 and before[4] == 1
    assert db.scalar(select(func.count()).select_from(EmailDelivery)) == 0
    assert db.get(AppSetting, 'feed.events.epoch') is not None
    revision = db.scalar(select(DirectFeedRevision))
    assert revision.source_bytes == doc['raw']
    receipt = db.scalar(select(worker.DirectFeedPublication))
    assert receipt.source_hash == hashlib.sha256(doc['raw']).hexdigest()
    assert sorted(json.loads(receipt.report_json)['event_ids']) == sorted(db.scalars(select(Event.id)))
    assert worker.publish_document(db, document_id)['inserted_events'] == 0
    assert worker.publish_batch(db)['processed'] == 0
    assert counts(db) == before


@pytest.mark.parametrize('fault', ['enrichment', 'cache', 'receipt'])
def test_failure_rolls_back_events_canonical_rows_receipt_and_jobs(db, monkeypatch, fault):
    document_id, _ = stage(db)
    activate(db)
    def fail(*a, **k):
        raise RuntimeError('injected interruption')
    if fault == 'enrichment':
        monkeypatch.setattr(worker, 'enqueue_feed_pnl_enrichment_for_event', fail)
    elif fault == 'cache':
        monkeypatch.setattr(worker, 'bump_feed_events_epoch', fail)
    else:
        def before_flush(session, *a):
            if any(isinstance(row, worker.DirectFeedPublication) for row in session.new):
                fail()
        event.listen(db, 'before_flush', before_flush)
    with pytest.raises(RuntimeError, match='injected'):
        worker.publish_document(db, document_id)
    assert counts(db) == [0, 0, 0, 0, 0]
    assert db.get(AppSetting, 'feed.events.epoch') is None


def test_unselected_direct_and_selected_legacy_ingest_make_no_requests(db, monkeypatch):
    import app.ingest_insider_trades as legacy
    document_id, _ = stage(db)
    with pytest.raises(FeedSourceMismatch):
        worker.publish_document(db, document_id)
    activate(db)
    monkeypatch.setattr(legacy, 'SessionLocal', sessionmaker(bind=db.get_bind()))
    monkeypatch.setattr(legacy, 'fetch_insider_trades', lambda **k: pytest.fail('FMP request after cutover'))
    assert legacy.ingest_insider_trades()['reason'] == 'FeedSourceMismatch'
    assert counts(db) == [0, 0, 0, 0, 0]


def test_pause_is_atomic_and_cannot_reset_boundary_or_restore_fmp(db):
    activate(db)
    with pytest.raises(ValueError, match='generation'):
        activate(db, provider='paused', generation=0)
    db.rollback()
    with pytest.raises(ValueError, match='immutable'):
        activate(db, provider='paused', generation=1, since=date(2026, 6, 2))
    db.rollback()
    with pytest.raises(ValueError, match='Select'):
        activate(db, provider='fmp', generation=1)
    db.rollback()
    activate(db, provider='paused', generation=1)
    with pytest.raises(FeedSourceMismatch):
        with writer_transaction(db, 'sec_form4', 'sec_edgar'):
            pytest.fail('Paused direct source executed')
    with pytest.raises(FeedSourceMismatch):
        with writer_transaction(db, 'sec_form4', 'fmp'):
            pytest.fail('Paused feed resumed FMP')
    activate(db, generation=2)


@pytest.mark.parametrize('fault', ['bytes', 'quarantine', 'missing', 'url', 'metadata'])
def test_corrupt_and_unapproved_sources_never_publish(db, fault):
    document_id, _ = stage(db)
    activate(db)
    doc = db.get(DirectFeedDocument, document_id)
    revision = db.scalar(select(DirectFeedRevision))
    if fault == 'bytes':
        revision.source_bytes += b'tampered'
    elif fault == 'quarantine':
        doc.status = 'quarantined'
    elif fault == 'missing':
        revision.source_bytes = None
    elif fault == 'url':
        metadata = json.loads(doc.metadata_json)
        metadata['url'] = 'https://example.com/filing.txt'
        doc.metadata_json = json.dumps(metadata)
    else:
        doc.source_key = 'other'
    db.commit()
    if fault in {'bytes', 'url', 'metadata'}:
        with pytest.raises(ValueError):
            worker.publish_document(db, document_id)
    else:
        assert worker.publish_document(db, document_id)['status'] == 'held'
    assert counts(db)[:4] == [0, 0, 0, 0]


def test_held_original_bytes_recollection_keeps_one_revision_and_can_retry(db):
    document_id, doc = stage(db)
    activate(db)
    revision = db.scalar(select(DirectFeedRevision))
    revision.source_bytes = None
    db.commit()
    assert worker.publish_document(db, document_id)['status'] == 'held'
    assert worker.publish_batch(db)['processed'] == 0
    source, parsed, _ = parse_document(doc['feed'], doc['raw'], doc['metadata'])
    record_document(db, db.get(DirectFeedDocument, document_id), doc['raw'], source, parsed)
    db.commit()
    assert worker.publish_batch(db, retry_held=True)['inserted_events'] == 4
    assert db.scalar(select(func.count()).select_from(DirectFeedRevision)) == 1


def test_old_backlog_does_not_consume_the_current_publication_batch(db):
    old_id, doc = stage(db)
    newer = {**doc['metadata'], 'key':'0000320193-26-000002', 'filing_date':'2026-06-04',
        'url':'https://www.sec.gov/Archives/edgar/data/320193/0000320193-26-000002.txt'}
    raw = doc['raw'].replace(b'0000320193-26-000001', b'0000320193-26-000002').replace(b'20260603', b'20260604')
    source, parsed, reasons = parse_document('sec_form4',raw,newer)
    assert not reasons
    row = discover(db,'sec_form4',newer)
    record_document(db,row,raw,source,parsed)
    current_id = row.id
    db.commit()
    activate(db,since=date(2026,6,4))
    result = worker.publish_batch(db,limit=1)
    assert result['processed'] == 1 and result['inserted_events'] == 4
    assert result['results'][0]['document_id'] == current_id
    assert db.scalar(select(worker.DirectFeedPublication).where(worker.DirectFeedPublication.document_id==old_id)) is None
    assert worker.publish_batch(db,limit=1,retry_held=True)['processed'] == 0


def test_incomplete_filing_day_does_not_consume_batch_or_create_hold(db):
    from zoneinfo import ZoneInfo
    document_id, _ = stage(db)
    row = db.get(DirectFeedDocument, document_id)
    metadata = json.loads(row.metadata_json)
    metadata['filing_date'] = datetime.now(timezone.utc).astimezone(ZoneInfo('America/New_York')).date().isoformat()
    row.metadata_json = json.dumps(metadata)
    db.commit()
    activate(db)
    assert worker.publish_batch(db)['processed'] == 0
    assert db.scalar(select(func.count()).select_from(worker.DirectFeedPublication)) == 0


def test_source_edit_after_publication_is_held_without_replacing_receipt(db):
    document_id, _ = stage(db)
    activate(db)
    worker.publish_document(db, document_id)
    original = db.scalar(select(worker.DirectFeedPublication)).report_json
    db.get(DirectFeedDocument, document_id).content_hash = 'changed'
    db.commit()
    assert worker.publish_document(db, document_id)['status'] == 'held'
    assert db.scalar(select(worker.DirectFeedPublication)).report_json == original
    assert counts(db)[:3] == [1, 4, 4]


def test_changed_canonical_rows_do_not_receive_false_repeat_verification(db):
    document_id, _ = stage(db)
    activate(db)
    worker.publish_document(db, document_id)
    db.scalar(select(InsiderTransactionNormalized)).price = 999
    db.commit()
    assert worker.publish_document(db, document_id)['status'] == 'held'
    assert counts(db)[:3] == [1, 4, 4]


def test_late_form4_publication_uses_arrival_for_execution_and_monitoring(db, monkeypatch):
    from app.services import direct_feed_publication as projection
    from app.services.backtesting.queries import event_entry_date
    from app.services.replicated_portfolios import _event_public_date
    from app.services.event_availability import availability_timestamp_expr
    clock = datetime(2026, 10, 9, 0, 30, tzinfo=timezone.utc)
    class FixedDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return clock if tz is not None else clock.replace(tzinfo=None)
    monkeypatch.setattr(projection, 'datetime', FixedDatetime)
    document_id, _ = stage(db)
    activate(db)
    worker.publish_document(db, document_id)
    rows = list(db.scalars(select(Event)))
    assert len(rows) == 4
    for row in rows:
        payload = json.loads(row.payload_json)
        assert payload['filing_date'] == '2026-06-03'
        assert row.event_date.date() == date(2026, 6, 3)
        assert row.ts.replace(tzinfo=timezone.utc) == clock
        assert event_entry_date(row, payload) == _event_public_date(row, payload) == clock.date()
    observed = availability_timestamp_expr(db, Event.event_date)
    assert list(db.scalars(select(Event.id).where(observed < datetime(2026, 10, 8)))) == []
    assert len(list(db.scalars(select(Event.id).where(observed >= datetime(2026, 10, 9))))) == 4
    assert worker.publish_document(db, document_id)['status'] == 'existing'
    assert all(row.ts.replace(tzinfo=timezone.utc) == clock for row in rows)


@pytest.mark.parametrize('field', ['ts', 'event_date'])
def test_changed_publication_dates_are_held_without_replacing_receipt(db, field):
    document_id, _ = stage(db)
    activate(db)
    worker.publish_document(db, document_id)
    receipt = db.scalar(select(worker.DirectFeedPublication)).report_json
    row = db.scalar(select(Event))
    setattr(row, field, getattr(row, field) - timedelta(days=1))
    db.commit()
    assert worker.publish_document(db, document_id)['status'] == 'held'
    assert db.scalar(select(worker.DirectFeedPublication)).report_json == receipt


@pytest.mark.parametrize('fault', ['price', 'missing', 'new_lot'])
def test_adopted_legacy_population_cannot_silently_drift_on_repeat(db, fault):
    document_id, doc = stage(db)
    _, parsed, _ = parse_document(doc['feed'], doc['raw'], doc['metadata'])
    # Unique rows let the complete legacy population reconcile one-to-one.
    # The repeated identical lot in the standard fixture deliberately holds.
    unique_doc = form4_document()
    unique_doc['metadata']['url'] = doc['metadata']['url']
    source, parsed, _ = parse_document(unique_doc['feed'], unique_doc['raw'], unique_doc['metadata'])
    record_document(db, db.get(DirectFeedDocument, document_id), unique_doc['raw'], source, parsed)
    for row in parsed['transactions']:
        db.add(InsiderTransactionNormalized(**row))
    db.commit()
    activate(db)
    assert worker.publish_document(db, document_id)['status'] == 'existing'
    assert worker.publish_document(db, document_id)['status'] == 'existing'
    receipt = db.scalar(select(worker.DirectFeedPublication)).report_json
    row = db.scalar(select(InsiderTransactionNormalized))
    if fault == 'price':
        row.price = 999
    elif fault == 'missing':
        db.delete(row)
    else:
        extra = {c.name:getattr(row,c.name) for c in row.__table__.columns if c.name not in {'id','created_at','updated_at'}}
        db.add(InsiderTransactionNormalized(**{**extra, 'normalized_hash':'additional-legacy-lot'}))
    db.commit()
    assert worker.publish_document(db, document_id)['status'] == 'held'
    assert db.scalar(select(worker.DirectFeedPublication)).report_json == receipt
    assert db.scalar(select(func.count()).select_from(Event)) == 0


@pytest.mark.parametrize('fault', ['missing', 'quarantine', 'tampered'])
def test_success_receipt_is_preserved_when_later_staging_loses_verification(db, fault):
    document_id, _ = stage(db)
    activate(db)
    worker.publish_document(db, document_id)
    receipt = db.scalar(select(worker.DirectFeedPublication)).report_json
    revision = db.scalar(select(DirectFeedRevision))
    if fault == 'missing':
        revision.source_bytes = None
    elif fault == 'quarantine':
        db.get(DirectFeedDocument, document_id).status = 'quarantined'
    else:
        revision.source_bytes += b'tampered'
    db.commit()
    if fault == 'tampered':
        with pytest.raises(ValueError, match='checksum'):
            worker.publish_document(db, document_id)
    else:
        assert worker.publish_document(db, document_id)['status'] == 'held'
    assert receipt == db.scalar(select(worker.DirectFeedPublication)).report_json
    assert counts(db)[:3] == [1, 4, 4]


def test_failure_can_retry_without_duplicate_rows_or_jobs(db, monkeypatch):
    document_id, _ = stage(db)
    activate(db)
    original = worker.bump_feed_events_epoch
    def fail(**kwargs): raise RuntimeError('temporary failure')
    monkeypatch.setattr(worker, 'bump_feed_events_epoch', fail)
    with pytest.raises(RuntimeError):
        worker.publish_document(db, document_id)
    monkeypatch.setattr(worker, 'bump_feed_events_epoch', original)
    assert worker.publish_document(db, document_id)['inserted_events'] == 4
    before = counts(db)
    assert worker.publish_document(db, document_id)['inserted_events'] == 0
    assert counts(db) == before


def test_sqlite_independent_writer_cannot_switch_until_first_transaction_ends(tmp_path):
    from sqlalchemy.exc import OperationalError
    engine = create_engine('sqlite:///' + (tmp_path / 'writer.sqlite').as_posix(), connect_args={'timeout': 0.05})
    Base.metadata.create_all(engine)
    with Session(engine) as first, Session(engine) as second:
        with writer_transaction(first, 'sec_form4', 'fmp'):
            with pytest.raises(OperationalError, match='locked'):
                activate(second)
            second.rollback()
        activate(second)
        with pytest.raises(FeedSourceMismatch):
            with writer_transaction(first, 'sec_form4', 'fmp'):
                pytest.fail('Stale provider selection survived commit')
    engine.dispose()


def test_legacy_helpers_are_closed_after_activation(db):
    from app.services.sec_form4 import stage_form4_shadow, promote_form4_shadow_events
    activate(db)
    for call in (lambda: stage_form4_shadow(db, xml_text='bad'), lambda: promote_form4_shadow_events(db)):
        with pytest.raises(FeedSourceMismatch):
            call()
        db.rollback()


def test_missing_control_schema_fails_closed_instead_of_defaulting_to_fmp(db, monkeypatch):
    import app.ingest_insider_trades as legacy
    from sqlalchemy.exc import OperationalError
    db.rollback()
    FeedSourceControl.__table__.drop(db.get_bind())
    monkeypatch.setattr(legacy, 'SessionLocal', sessionmaker(bind=db.get_bind()))
    monkeypatch.setattr(legacy, 'fetch_insider_trades', lambda **k: pytest.fail('Missing control allowed FMP'))
    with pytest.raises(OperationalError):
        legacy.ingest_insider_trades()


def test_schema_adds_nullable_source_bytes_and_is_repeatable():
    engine = create_engine('sqlite:///:memory:')
    with engine.begin() as conn:
        conn.execute(text('CREATE TABLE direct_feed_revisions (id INTEGER PRIMARY KEY, document_id INTEGER, '
                          'content_hash TEXT, source_text TEXT, parsed_json TEXT, fetched_at DATETIME)'))
        conn.execute(text("INSERT INTO direct_feed_revisions VALUES (1, 2, 'sha', 'extracted', '{}', NULL)"))
    ensure_direct_feed_schema(engine)
    ensure_direct_feed_schema(engine)
    with engine.connect() as conn:
        assert conn.execute(text('SELECT source_text, source_bytes FROM direct_feed_revisions')).one() == ('extracted', None)
    assert 'feed_source_controls' in inspect(engine).get_table_names()
    engine.dispose()


def test_postgres_contention_fails_before_any_source_or_canonical_query():
    class Bind:
        class dialect:
            name = 'postgresql'
    class Busy:
        def get_bind(self): return Bind()
        def scalar(self, statement, params):
            assert 'pg_try_advisory_xact_lock' in str(statement)
            assert params == {'key': 84193648}
            return False
    with pytest.raises(FeedWriterBusy):
        lock_feed(Busy(), 'sec_form4')


@pytest.mark.parametrize('mode', ['off', 'paused'])
def test_job_disabled_or_paused_never_opens_database(monkeypatch, capsys, mode):
    from app.jobs import publish_direct_feeds as job
    monkeypatch.setattr('sys.argv', ['publish_direct_feeds'])
    monkeypatch.setenv('DIRECT_FEED_PUBLICATION_ENABLED', 'true' if mode == 'paused' else 'false')
    monkeypatch.setenv('BACKGROUND_JOBS_PAUSED', 'true' if mode == 'paused' else 'false')
    monkeypatch.setattr(job, 'SessionLocal', lambda: pytest.fail('Inactive job opened database'))
    job.main()
    result = json.loads(capsys.readouterr().out)
    assert result.get('status') == 'disabled' or result.get('reason') == 'background_jobs_paused'
