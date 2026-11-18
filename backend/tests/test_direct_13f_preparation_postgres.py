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
