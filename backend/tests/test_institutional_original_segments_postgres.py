"""Actual PostgreSQL row-version binding and complete original transport."""
import io
import gzip
import hashlib
import json
from pathlib import Path

import pytest
from sqlalchemy import insert, select, text
from sqlalchemy.orm import Session

from app import db as database
from app.jobs import capture_direct_13f_evidence as job
from app.services import institutional_original_segments as pieces
from app.services.institutional_capture_transport import StreamReceiver, decode, encode
from app.services.direct_feed_store import DirectFeedDocument, DirectFeedRevision
from test_direct_13f_preparation_postgres import pg
from test_direct_13f_worker import pair, stage


def freeze(engine):
    with engine.connect().execution_options(isolation_level='REPEATABLE READ') as connection:
        with connection.begin():
            connection.execute(text('SET TRANSACTION READ ONLY'))
            with Session(bind=connection) as db:
                return pieces.make_plan(pieces.snapshot_rows(db), snapshot_at=100)


def seed(engine):
    with Session(engine) as db:
        for original in pair():
            stage(db, original)


def test_actual_readonly_original_export_and_independent_full_parse(pg, monkeypatch):
    seed(pg)
    plan = freeze(pg)
    monkeypatch.setattr(database, 'engine', pg)
    output = io.BytesIO()
    receipt = job.export_snapshot(output, plan['pieces'][0])
    destination = io.BytesIO()
    reader = StreamReceiver(destination, original_piece=plan['pieces'][0])
    reader.read(io.BytesIO(output.getvalue()))
    verified = reader.finish(0)
    assert reader.receipt == decode(encode(receipt))
    assert verified['originals_verified'] == 2
    assert verified['acceptance'] == pieces.ACCEPTANCE
    assert freeze(pg) == plan  # No source or metadata write during export.


@pytest.mark.parametrize('change', ['document', 'revision', 'outside_piece'])
def test_changed_row_versions_and_whole_inventory_fail_closed(pg, monkeypatch, change):
    seed(pg)
    monkeypatch.setattr(database, 'engine', pg)
    if change == 'outside_piece':
        monkeypatch.setattr(pieces, 'PIECE_DOCUMENTS', 1)
    plan = freeze(pg)
    with Session(pg) as db:
        if change == 'revision':
            revision = db.scalar(select(DirectFeedRevision).order_by(DirectFeedRevision.id))
            revision.parsed_json = '{}'
        else:
            documents = list(db.scalars(select(DirectFeedDocument).order_by(DirectFeedDocument.id)))
            documents[-1 if change == 'outside_piece' else 0].error = 'changed after freeze'
        db.commit()
    output = io.BytesIO()
    with pytest.raises(ValueError, match='source_inventory_changed'):
        job.export_snapshot(output, plan['pieces'][0])
    assert output.getvalue() == b''


def test_changed_original_with_saved_hash_is_rejected_even_in_new_plan(pg, monkeypatch):
    seed(pg)
    with Session(pg) as db:
        revision = db.scalar(select(DirectFeedRevision).order_by(DirectFeedRevision.id))
        revision.source_bytes = revision.source_bytes + b'changed'
        db.commit()
    plan = freeze(pg)
    monkeypatch.setattr(database, 'engine', pg)
    with pytest.raises(ValueError, match='original_hash_mismatch'):
        job.export_snapshot(io.BytesIO(), plan['pieces'][0])


def test_full_large_retained_original_survives_actual_pg_and_receiver(pg, monkeypatch):
    fixture = Path(__file__).parent / 'fixtures/sec_13f_preparation_buffers'
    original = json.loads((fixture / 'jpm-2026q2-metadata.json').read_bytes())
    original['raw'] = gzip.decompress((fixture / 'jpm-2026q2.txt.gz').read_bytes())
    assert hashlib.sha256(original['raw']).hexdigest() == original['content_hash']
    assert len(original['raw']) > 19_000_000
    with Session(pg) as db:
        stage(db, original)
    plan = freeze(pg)
    monkeypatch.setattr(database, 'engine', pg)
    output = io.BytesIO()
    job.export_snapshot(output, plan['pieces'][0])
    reader = StreamReceiver(io.BytesIO(), original_piece=plan['pieces'][0])
    reader.read(io.BytesIO(output.getvalue()))
    result = reader.finish(0)
    assert result['originals_verified'] == 1
    assert result['source_bytes'] == len(original['raw'])
    assert len(output.getvalue()) <= pieces.PIECE_BYTES


def test_actual_inventory_beyond_old_export_cap_is_not_truncated(pg):
    with Session(pg) as db:
        for start in range(1, 10002, 250):
            stop = min(start + 250, 10002)
            db.execute(insert(DirectFeedDocument), [dict(id=i, feed='sec_13f',
                source_key='failed-fixture-'+str(i), source_url='https://example.test/'+str(i),
                metadata_json='{}', status='failed') for i in range(start, stop)])
        db.commit()
    plan = freeze(pg)
    assert plan['document_count'] == 10001 and plan['source_bytes'] == 0
    assert [row[0] for piece in plan['pieces'] for row in piece['rows']] == list(range(1, 10002))
