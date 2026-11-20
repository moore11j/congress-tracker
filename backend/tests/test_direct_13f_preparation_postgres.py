"""Real row-version, concurrency and >10k inventory contracts in an owned DB."""
from contextlib import contextmanager
import hashlib
import json
import os
from unittest.mock import patch
import uuid

import pytest
from sqlalchemy import create_engine, insert, select, text
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session

from app.db import Base
from app.services import direct_13f_preparation as preparation
from app.services import direct_13f_batch as batch
from app.services.direct_feed_store import DirectFeedDocument, DirectFeedRevision
from app.services.direct_feed_worker import DirectFeedPublication
from app.services.feed_source_control import select_feed_source
from test_direct_13f_publication import document, mappings, SINCE
from test_direct_13f_worker import pair, stage


@pytest.fixture
def pg(monkeypatch):
    url = os.getenv('INSTITUTIONAL_TEST_POSTGRES_URL')
    if not url:
        pytest.skip('Requires owned PostgreSQL')
    parsed = make_url(url)
    assert os.getenv('CI') == 'true' and parsed.host in {'127.0.0.1', 'localhost'}
    assert parsed.database == 'institutional_preparation_test'
    def denied(*args, **kwargs):
        pytest.fail('External transport in owned rehearsal')
    monkeypatch.setattr('requests.sessions.Session.request', denied)
    monkeypatch.setattr('httpx.Client.send', denied)
    schema = 'prep_' + uuid.uuid4().hex
    admin = create_engine(url)
    with admin.begin() as connection:
        connection.execute(text('CREATE SCHEMA ' + schema))
    engine = create_engine(url, connect_args={'options': '-c search_path='+schema+',public -c statement_timeout=10000'})
    try:
        Base.metadata.create_all(engine)
        preparation.ensure_prepared_13f_schema(engine)
        preparation.ensure_prepared_13f_schema(engine)
        yield engine
    finally:
        engine.dispose()
        with admin.begin() as connection:
            connection.execute(text('DROP SCHEMA '+schema+' CASCADE'))
        admin.dispose()


def warm(db):
    for _ in range(20):
        result = preparation.advance_preparation(db, max_documents=1)
        db.commit()
        if result['status'] == 'sweep_complete':
            return
    pytest.fail('Incomplete fixture')


def test_real_source_versions_and_resume(pg):
    with Session(pg) as db:
        first = stage(db, pair()[0])
        warm(db)
        original = preparation.preparation_fingerprint(db)
        db.rollback()
    with Session(pg) as db:
        assert preparation.preparation_fingerprint(db) == original
        with patch.object(preparation, 'verified_peer_records', side_effect=AssertionError('Unchanged parse repeated')):
            assert preparation.advance_preparation(db)['processed'] == 0
            db.commit()
        revision = db.scalar(select(DirectFeedRevision))
        # Same bytes, different database row version: rehash but reuse parse.
        revision.parsed_json = '{}'
        db.commit()
        with pytest.raises(ValueError, match='pending'):
            preparation.preparation_fingerprint(db)
        db.rollback()
        result = preparation.advance_preparation(db)
        assert result['reused'] == 1 and result['parsed'] == 0
        db.commit()
        assert preparation.preparation_fingerprint(db)['documents'] == 1
        revision.source_bytes = revision.source_bytes.replace(b'10000000', b'20000000')
        db.commit()
        with pytest.raises(ValueError, match='pending'):
            preparation.preparation_fingerprint(db)
        db.rollback()
        with pytest.raises(ValueError, match='checksum'):
            preparation.advance_preparation(db)
        db.rollback()


def test_real_chunk_lock_and_reader_source_lock(pg):
    with Session(pg) as first, Session(pg) as second:
        stage(first, pair()[0])
        preparation.advance_preparation(first)
        assert preparation.advance_preparation(second) == {'status': 'busy', 'processed': 0}
        second.rollback()
        first.commit()
        warm(first)
        first.execute(text('LOCK TABLE direct_feed_documents, direct_feed_revisions, prepared_13f_parses IN SHARE MODE'))
        second.execute(text("SET LOCAL lock_timeout='50ms'"))
        from sqlalchemy.exc import OperationalError
        with pytest.raises(OperationalError):
            second.execute(text("UPDATE direct_feed_revisions SET parsed_json='{}'"))
        second.rollback()
        first.rollback()


def test_actual_postgres_prepared_publication_and_repeat(pg):
    with Session(pg) as db:
        mappings(db)
        select_feed_source(db, feed='sec_13f', provider='sec_edgar', publish_since=SINCE,
                           expected_generation=0, reason='Owned fixture')
        db.commit()
        for doc in pair():
            stage(db, doc)
        warm(db)
        result = batch.publish_13f_batch(db, identifier_documents=[], use_prepared_peers=True)
        assert result['processed'] == 2 and result['feed_events'] > 0
        assert batch.publish_13f_batch(db, identifier_documents=[], use_prepared_peers=True)['processed'] == 0


def test_bounded_job_preserves_committed_chunks_on_interruption(pg):
    from sqlalchemy.orm import sessionmaker
    from app.jobs.prepare_direct_13f_peers import prepare_chunks
    factory = sessionmaker(bind=pg)
    with factory() as db:
        for serial in range(1, 31):
            stage(db, document(serial=serial))
    def interrupt_after_first_commit():
        with factory() as observer:
            from sqlalchemy import func
            if observer.scalar(select(func.count()).select_from(preparation.Prepared13FParse)) >= 25:
                raise RuntimeError('Simulated stop between transactions')
    with pytest.raises(RuntimeError, match='Simulated stop'):
        prepare_chunks(factory, checkpoint=interrupt_after_first_commit)
    with factory() as db:
        assert db.get(preparation.Prepared13FProgress, 'peers').cursor == 25
    result = prepare_chunks(factory, checkpoint=lambda: None)
    assert result['processed'] == 5 and result['status'] == 'sweep_complete'
    with factory() as db:
        assert preparation.preparation_fingerprint(db)['documents'] == 30


def test_memory_boundary_returns_only_committed_progress_and_resumes(pg):
    from sqlalchemy import func
    from sqlalchemy.orm import sessionmaker
    from app.jobs.prepare_direct_13f_peers import prepare_chunks, PreparationMemoryHold
    factory = sessionmaker(bind=pg)
    with factory() as db:
        for serial in range(1, 31):
            stage(db, document(serial=serial))
    def memory_after_commit():
        with factory() as observer:
            if observer.scalar(select(func.count()).select_from(preparation.Prepared13FParse)) == 25:
                raise PreparationMemoryHold('memory_headroom')
    result = prepare_chunks(factory, checkpoint=memory_after_commit)
    assert result['status'] == 'memory_deferred'
    assert result['processed'] == result['parsed'] == 25
    assert result['chunks'] == 1 and result['cursor'] == 25
    assert result['publication_writes'] == 0
    with factory() as db:
        assert db.get(preparation.Prepared13FProgress, 'peers').cursor == 25
        assert db.scalar(select(func.count()).select_from(preparation.Prepared13FParse)) == 25
        with pytest.raises(ValueError, match='pending'):
            preparation.preparation_fingerprint(db)
    result = prepare_chunks(factory, checkpoint=lambda: None)
    assert result['processed'] == 5 and result['status'] == 'sweep_complete'
    with factory() as db:
        assert preparation.preparation_fingerprint(db)['documents'] == 30
        assert db.scalar(select(func.count()).select_from(DirectFeedPublication)) == 0


def test_memory_inside_chunk_still_raises_and_rolls_back(pg, monkeypatch):
    from sqlalchemy import func
    from sqlalchemy.orm import sessionmaker
    from app.jobs.prepare_direct_13f_peers import prepare_chunks, PreparationMemoryHold
    factory = sessionmaker(bind=pg)
    with factory() as db:
        for doc in pair():
            stage(db, doc)
    original = preparation.advance_preparation
    def stop_after_uncommitted_write(db, **kwargs):
        def checkpoint():
            if db.scalar(select(func.count()).select_from(preparation.Prepared13FParse)):
                raise PreparationMemoryHold('memory_headroom')
        return original(db, checkpoint=checkpoint)
    monkeypatch.setattr(preparation, 'advance_preparation', stop_after_uncommitted_write)
    with pytest.raises(PreparationMemoryHold, match='memory_headroom'):
        prepare_chunks(factory, checkpoint=lambda: None)
    with factory() as db:
        assert db.scalar(select(func.count()).select_from(preparation.Prepared13FParse)) == 0
        assert db.get(preparation.Prepared13FProgress, 'peers') is None


@pytest.mark.parametrize('boundary', ['between', 'inside', 'after_advance', 'alarm', 'database_alarm'])
def test_work_budget_rolls_back_only_uncommitted_chunk_and_resumes(pg, monkeypatch, boundary):
    import signal
    from sqlalchemy import delete, func
    from sqlalchemy.orm import sessionmaker
    from app.jobs.prepare_direct_13f_peers import (
        prepare_chunks, PreparationWorkBudgetHold, _stop_for_work_budget,
    )
    factory = sessionmaker(bind=pg)
    with factory() as db:
        for serial in range(1, 31):
            stage(db, document(serial=serial))
    prepare_chunks(factory, checkpoint=lambda: None)
    with factory() as db:
        expected = preparation.preparation_fingerprint(db)
        db.execute(delete(preparation.Prepared13FParse))
        db.execute(delete(preparation.Prepared13FProgress))
        db.commit()
    original = preparation.advance_preparation
    def interrupted(db, **kwargs):
        def checkpoint():
            count = db.scalar(select(func.count()).select_from(preparation.Prepared13FParse))
            if boundary in {'inside', 'alarm'} and count > 25:
                if boundary == 'alarm':
                    signal.raise_signal(signal.SIGALRM)
                raise PreparationWorkBudgetHold('work budget')
        result = original(db, checkpoint=checkpoint)
        if boundary == 'after_advance' and result['cursor'] == 0:
            raise PreparationWorkBudgetHold('work budget')
        if boundary == 'database_alarm' and result['cursor'] == 0:
            signal.setitimer(signal.ITIMER_REAL, .02)
            try:
                db.execute(text('SELECT pg_sleep(1)'))
            finally:
                signal.setitimer(signal.ITIMER_REAL, 0)
        return result
    def before_chunk():
        if boundary == 'between':
            with factory() as db:
                if db.scalar(select(func.count()).select_from(preparation.Prepared13FParse)) == 25:
                    raise PreparationWorkBudgetHold('work budget')
    monkeypatch.setattr(preparation, 'advance_preparation', interrupted)
    previous_handler = signal.signal(signal.SIGALRM, _stop_for_work_budget)
    try:
        result = prepare_chunks(factory, checkpoint=before_chunk)
    finally:
        signal.signal(signal.SIGALRM, previous_handler)
    assert result['status'] == 'budget_deferred'
    assert result['chunks'] == 1 and result['processed'] == result['parsed'] == 25
    assert result['cursor'] == 25 and result['publication_writes'] == 0
    with factory() as db:
        assert db.scalar(select(func.count()).select_from(preparation.Prepared13FParse)) == 25
        assert db.get(preparation.Prepared13FProgress, 'peers').cursor == 25
        with pytest.raises(ValueError, match='pending'):
            preparation.preparation_fingerprint(db)
    monkeypatch.setattr(preparation, 'advance_preparation', original)
    # PostgreSQL can observe the closed socket only after its current command
    # returns. Its transaction advisory lock must protect the old chunk until
    # then; a bounded busy response is safe, not permission to bypass the lock.
    import time
    deadline = time.monotonic() + 5
    while True:
        result = prepare_chunks(factory, checkpoint=lambda: None)
        if result['status'] != 'busy':
            break
        assert result['processed'] == 0 and result['publication_writes'] == 0
        assert time.monotonic() < deadline, 'Interrupted transaction lock was not released'
        time.sleep(.02)
    assert result['status'] == 'sweep_complete' and result['processed'] == 5
    with factory() as db:
        assert preparation.preparation_fingerprint(db) == expected
        assert db.scalar(select(func.count()).select_from(DirectFeedPublication)) == 0


def test_work_budget_inside_first_chunk_still_fails_and_rolls_back(pg, monkeypatch):
    from sqlalchemy import func
    from sqlalchemy.orm import sessionmaker
    from app.jobs.prepare_direct_13f_peers import prepare_chunks, PreparationWorkBudgetHold
    factory = sessionmaker(bind=pg)
    with factory() as db:
        for doc in pair():
            stage(db, doc)
    original = preparation.advance_preparation
    def interrupted(db, **kwargs):
        result = original(db, **kwargs)
        raise PreparationWorkBudgetHold('work budget')
    monkeypatch.setattr(preparation, 'advance_preparation', interrupted)
    with pytest.raises(PreparationWorkBudgetHold):
        prepare_chunks(factory, checkpoint=lambda: None)
    with factory() as db:
        assert db.scalar(select(func.count()).select_from(preparation.Prepared13FParse)) == 0
        assert db.get(preparation.Prepared13FProgress, 'peers') is None


@pytest.mark.parametrize('kind', ['parser', 'memory', 'cpu'])
def test_other_inside_chunk_errors_after_commit_remain_failures(pg, monkeypatch, kind):
    from sqlalchemy import func
    from sqlalchemy.orm import sessionmaker
    from app.jobs.prepare_direct_13f_peers import prepare_chunks, PreparationMemoryHold
    error = {'parser': ValueError('checksum'), 'memory': PreparationMemoryHold('memory_headroom'),
             'cpu': RuntimeError('hard CPU budget')}[kind]
    factory = sessionmaker(bind=pg)
    with factory() as db:
        for serial in range(1, 31):
            stage(db, document(serial=serial))
    original = preparation.advance_preparation
    def interrupted(db, **kwargs):
        result = original(db, **kwargs)
        if result['cursor'] == 0:
            raise error
        return result
    monkeypatch.setattr(preparation, 'advance_preparation', interrupted)
    with pytest.raises(type(error), match=str(error)):
        prepare_chunks(factory, checkpoint=lambda: None)
    with factory() as db:
        assert db.scalar(select(func.count()).select_from(preparation.Prepared13FParse)) == 25
        assert db.get(preparation.Prepared13FProgress, 'peers').cursor == 25


def test_budget_during_commit_is_not_reported_as_safe_defer(pg):
    from sqlalchemy import func
    from sqlalchemy.orm import sessionmaker
    from app.jobs.prepare_direct_13f_peers import prepare_chunks, PreparationWorkBudgetHold
    with Session(pg) as db:
        for serial in range(1, 31):
            stage(db, document(serial=serial))
    commits = 0
    class UncertainCommit(Session):
        def commit(self):
            nonlocal commits
            super().commit()
            commits += 1
            if commits == 2:
                raise PreparationWorkBudgetHold('uncertain commit acknowledgement')
    with pytest.raises(PreparationWorkBudgetHold, match='uncertain commit'):
        prepare_chunks(sessionmaker(bind=pg, class_=UncertainCommit), checkpoint=lambda: None)
    with Session(pg) as db:
        assert db.scalar(select(func.count()).select_from(preparation.Prepared13FParse)) == 30


def test_interrupted_driver_close_failure_is_not_reported_as_safe_defer(pg, monkeypatch):
    from sqlalchemy.orm import sessionmaker
    from app.jobs.prepare_direct_13f_peers import prepare_chunks, PreparationWorkBudgetHold
    factory = sessionmaker(bind=pg)
    with factory() as db:
        for serial in range(1, 31):
            stage(db, document(serial=serial))
    original = preparation.advance_preparation
    driver = None
    def interrupted(db, **kwargs):
        nonlocal driver
        result = original(db, **kwargs)
        if result['cursor'] == 0:
            driver = db.connection().connection.driver_connection
            original_close = driver.close
            def failing_close():
                original_close()
                raise RuntimeError('driver close acknowledgement failed')
            monkeypatch.setattr(driver, 'close', failing_close)
            raise PreparationWorkBudgetHold('work budget')
        return result
    monkeypatch.setattr(preparation, 'advance_preparation', interrupted)
    try:
        with pytest.raises(RuntimeError, match='driver close acknowledgement failed'):
            prepare_chunks(factory, checkpoint=lambda: None)
    finally:
        monkeypatch.undo()
    with factory() as db:
        assert db.get(preparation.Prepared13FProgress, 'peers').cursor == 25


def test_prepared_capacity_is_readonly_with_legacy_source_selected(pg, monkeypatch):
    from app.jobs.check_direct_13f_capacity import prepare_readonly
    from app.services.feed_source_control import FeedSourceControl
    monkeypatch.setattr('app.services.direct_13f_identifiers.load_staged_identifiers', lambda *args: [])
    with Session(pg) as db:
        select_feed_source(db, feed='sec_13f', provider='sec_edgar', publish_since=SINCE,
                           expected_generation=0, reason='Owned fixture')
        db.commit()
        db.get(FeedSourceControl, 'sec_13f').provider = 'fmp'
        db.commit()
        for doc in pair():
            stage(db, doc)
        warm(db)
        db.execute(text('SET TRANSACTION READ ONLY'))
        result = prepare_readonly(db, checkpoint=lambda phase: None, identifier_manifest='unused', use_prepared_peers=True)
        assert result['status'] == 'prepared' and result['source_provider'] == 'fmp'
        assert result['peer_fingerprint']['documents'] == 2
        assert db.scalar(text('SHOW transaction_read_only')) == 'on'
        db.rollback()


def test_complete_10001_originals_in_bounded_chunks(pg):
    total = 10001
    with Session(pg) as db:
        for start in range(1, total+1, 250):
            documents, revisions = [], []
            for index in range(start, min(start+250, total+1)):
                source = document(serial=index)
                metadata = source['metadata']
                documents.append(dict(id=index, feed='sec_13f', source_key=metadata['key'],
                    source_url=metadata['url'], metadata_json=json.dumps(metadata),
                    content_hash=source['content_hash'], status='parsed'))
                revisions.append(dict(id=index, document_id=index, content_hash=source['content_hash'],
                    source_text='', source_bytes=source['raw'], parsed_json='{}'))
            db.execute(insert(DirectFeedDocument), documents)
            db.execute(insert(DirectFeedRevision), revisions)
            db.commit()
        maximum, processed, chunks = 0, 0, 0
        for _ in range(402):
            result = preparation.advance_preparation(db)
            db.commit()
            chunks += 1
            maximum = max(maximum, result['processed'])
            processed += result['processed']
            if result['status'] == 'sweep_complete':
                break
        assert processed == total and maximum <= 25 and chunks == 401
        assert preparation.preparation_fingerprint(db)['documents'] == total
        assert sum(1 for _ in preparation.prepared_records(db)) == total
        with patch.object(preparation, 'verified_peer_records', side_effect=AssertionError('Repeat parsed originals')):
            assert preparation.advance_preparation(db)['processed'] == 0
            db.commit()
