import hashlib
import json
from unittest.mock import patch

import pytest
from sqlalchemy import func, select

from app.models import Event
from app.services import direct_13f_preparation as preparation, direct_13f_batch as batch
from app.services.direct_feed_store import DirectFeedDocument, DirectFeedRevision
from app.services.direct_feed_worker import DirectFeedPublication
from test_direct_13f_worker import db, stage, pair
from test_direct_13f_publication import document


def warm(db, **kwargs):
    for _ in range(10):
        result = preparation.advance_preparation(db, **kwargs)
        db.commit()
        if result['status'] == 'sweep_complete':
            return result
    pytest.fail('Preparation did not finish')


def test_resume_and_complete_exact_peer_parity(db):
    ids = [stage(db, doc) for doc in [*pair(), document(serial=3)]]
    first = preparation.advance_preparation(db, max_documents=1)
    assert first['processed'] == first['parsed'] == 1 and first['status'] == 'preparing'
    db.commit()
    with pytest.raises(ValueError, match='pending'):
        preparation.preparation_fingerprint(db)
    db.rollback()
    warm(db, max_documents=1)
    before = preparation.preparation_fingerprint(db)
    with patch.object(preparation, 'verified_peer_records', side_effect=AssertionError('reparse')):
        assert warm(db)['processed'] == 0
    assert preparation.preparation_fingerprint(db) == before
    old = batch.prepare_13f_batch(db, ids=ids, identifier_documents=[])
    new = batch.prepare_13f_batch(db, ids=ids, identifier_documents=[], use_prepared_peers=True)
    try:
        assert old['summary'] == new['summary']
        for period in old['groups']:
            assert old['groups'][period].records == new['groups'][period].records
    finally:
        for prepared in (old, new):
            for group in prepared['groups'].values():
                group.close()


@pytest.mark.parametrize('reuse', [False, True])
def test_full_parse_record_is_released_before_next_checkpoint(db, monkeypatch, reuse):
    import weakref
    for source in pair():
        stage(db, source)
    if reuse:
        warm(db)
        # Same immutable original with a changed source token requires rehash
        # and compact-record reuse, without reparsing the original.
        for cached in db.scalars(select(preparation.Prepared13FParse)):
            cached.source_token = 'previous source token'
        db.commit()
    observed = []
    if reuse:
        original = preparation._decode
        def decode(*args, **kwargs):
            record = original(*args, **kwargs)
            observed.append(weakref.ref(record))
            return record
        monkeypatch.setattr(preparation, '_decode', decode)
    else:
        original = preparation.verified_peer_records
        def records(documents):
            for record in original(documents):
                observed.append(weakref.ref(record))
                yield record
        monkeypatch.setattr(preparation, 'verified_peer_records', records)
    checkpoints_after_parse = []
    def checkpoint():
        if observed:
            checkpoints_after_parse.append(True)
            assert all(ref() is None for ref in observed)
    result = preparation.advance_preparation(db, max_documents=1, checkpoint=checkpoint)
    db.commit()
    assert checkpoints_after_parse and result['processed'] == 1
    assert result['reused' if reuse else 'parsed'] == 1


@pytest.mark.parametrize('change', ['bytes', 'metadata', 'missing_original', 'arrival', 'payload', 'parser'])
def test_changed_evidence_cannot_publish(db, monkeypatch, change):
    docs = pair()
    first = stage(db, docs[0])
    warm(db)
    if change == 'bytes':
        revision = db.scalar(select(DirectFeedRevision))
        revision.source_bytes = revision.source_bytes.replace(b'10000000', b'20000000')
        assert hashlib.sha256(revision.source_bytes).hexdigest() != revision.content_hash
    elif change == 'metadata':
        row = db.get(DirectFeedDocument, first)
        row.metadata_json = json.dumps({**json.loads(row.metadata_json), 'filing_date': '2099-01-01'})
    elif change == 'missing_original':
        db.scalar(select(DirectFeedRevision)).source_bytes = None
    elif change == 'arrival':
        stage(db, docs[1])
    elif change == 'payload':
        db.get(preparation.Prepared13FParse, first).payload_json = 'null'
    else:
        monkeypatch.setattr(preparation, 'parser_version', lambda: 'changed')
    db.commit()
    with pytest.raises(ValueError):
        batch.publish_13f_batch(db, identifier_documents=[], use_prepared_peers=True)
    db.rollback()
    assert db.scalar(select(func.count()).select_from(Event)) == 0
    assert db.scalar(select(func.count()).select_from(DirectFeedPublication)) == 0


def test_uncommitted_chunk_rolls_back_progress(db):
    for doc in pair():
        stage(db, doc)
    preparation.advance_preparation(db, max_documents=1)
    db.rollback()
    assert db.scalar(select(func.count()).select_from(preparation.Prepared13FParse)) == 0
    assert db.get(preparation.Prepared13FProgress, 'peers') is None
    warm(db)
    assert preparation.preparation_fingerprint(db)['documents'] == 2


def test_prepared_publication_preserves_events_and_repeat(db):
    for doc in pair():
        stage(db, doc)
    warm(db)
    result = batch.publish_13f_batch(db, identifier_documents=[], use_prepared_peers=True)
    assert result['processed'] == 2 and result['feed_events'] > 0
    assert batch.publish_13f_batch(db, identifier_documents=[], use_prepared_peers=True)['processed'] == 0


def test_changed_inventory_between_preparation_and_write_aborts(db, monkeypatch):
    first = stage(db, pair()[0])
    warm(db)
    original = batch.prepare_13f_batch
    def changing(*args, **kwargs):
        prepared = original(*args, **kwargs)
        db.get(DirectFeedDocument, first).status = 'quarantined'
        db.commit()
        return prepared
    monkeypatch.setattr(batch, 'prepare_13f_batch', changing)
    with pytest.raises(ValueError):
        batch.publish_13f_batch(db, identifier_documents=[], use_prepared_peers=True)
    db.rollback()
    assert db.scalar(select(func.count()).select_from(DirectFeedPublication)) == 0


def test_disabled_job_does_not_touch_database(monkeypatch, capsys):
    from app.jobs import prepare_direct_13f_peers
    monkeypatch.delenv('DIRECT_13F_PEER_PREPARATION_ENABLED', raising=False)
    prepare_direct_13f_peers.main()
    assert json.loads(capsys.readouterr().out) == {'status': 'disabled'}


def test_memory_before_first_chunk_still_fails_without_opening_database():
    from app.jobs.prepare_direct_13f_peers import prepare_chunks, PreparationMemoryHold
    def unavailable():
        raise PreparationMemoryHold('memory_headroom')
    def forbidden_database():
        pytest.fail('Database opened while memory guard held')
    with pytest.raises(PreparationMemoryHold, match='memory_headroom'):
        prepare_chunks(forbidden_database, checkpoint=unavailable)


def test_preparation_requires_exclusive_queue_claim(tmp_path):
    import sqlite3
    from app.jobs.prepare_direct_13f_peers import require_preparation_queue, JOB_NAME
    path = tmp_path/'queue.sqlite3'
    with sqlite3.connect(path) as queue:
        queue.execute('CREATE TABLE jobs (key TEXT, token TEXT, pid INTEGER, lane TEXT, started_at INTEGER)')
        queue.execute('INSERT INTO jobs VALUES (?,?,?,?,?)', (JOB_NAME, 'owned', 123, 'data', 1))
    require_preparation_queue(path, 'owned', 123)
    with pytest.raises(RuntimeError):
        require_preparation_queue(path, 'other', 123)
    with sqlite3.connect(path) as queue:
        queue.execute('INSERT INTO jobs VALUES (?,?,?,?,?)', ('other', 'x', 456, 'data', 1))
    with pytest.raises(RuntimeError):
        require_preparation_queue(path, 'owned', 123)


def test_whole_original_larger_than_chunk_is_not_starved(db):
    first, second = [stage(db, doc) for doc in pair()]
    result = preparation.advance_preparation(db, max_bytes=1)
    db.commit()
    assert result['processed'] == 1 and result['cursor'] == first
    assert result['source_bytes'] > 1
    warm(db, max_bytes=1)
    assert preparation.preparation_fingerprint(db)['documents'] == 2


def test_arrival_behind_cursor_remains_pending_until_wrap(db):
    first, second = [stage(db, doc) for doc in pair()]
    db.get(DirectFeedDocument, first).status = 'quarantined'
    third = stage(db, document(serial=3))
    preparation.advance_preparation(db, max_documents=1)
    db.commit()
    db.get(DirectFeedDocument, first).status = 'parsed'
    db.commit()
    warm(db, max_documents=1)
    with pytest.raises(ValueError, match='pending'):
        preparation.preparation_fingerprint(db)
    db.rollback()
    warm(db, max_documents=1)
    assert preparation.preparation_fingerprint(db)['documents'] == 3
