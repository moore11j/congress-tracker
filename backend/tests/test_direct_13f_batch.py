import hashlib
import json

import pytest
from sqlalchemy import func, select

from app.models import Event
from app.services import direct_13f_batch as batch
from app.services.direct_feed_store import DirectFeedDocument, DirectFeedRevision
from app.services.direct_feed_worker import DirectFeedPublication
from test_direct_13f_worker import db, stage, pair


def test_captured_batch_publishes_and_empty_repeat_does_not_fetch(db):
    prior, current = pair()
    stage(db, prior)
    stage(db, current)
    result = batch.publish_13f_batch(db, identifier_documents=[], limit=2)
    assert result['processed'] == 2 and result['feed_events'] > 0
    assert batch.publish_13f_batch(db, identifier_documents=[])['processed'] == 0
    assert db.scalar(select(func.count()).select_from(DirectFeedPublication)) == 2


def test_out_of_order_batch_requires_explicit_waiting_retry(db):
    prior, current = pair()
    stage(db, current)
    first = batch.publish_13f_batch(db, identifier_documents=[])
    assert first['status'] == 'partial' and first['results'][0]['status'] == 'waiting'
    stage(db, prior)
    assert batch.publish_13f_batch(db, identifier_documents=[])['processed'] == 1
    assert batch.publish_13f_batch(db, identifier_documents=[])['processed'] == 0
    retry = batch.publish_13f_batch(db, identifier_documents=[], retry_waiting=True)
    assert retry['processed'] == 1 and retry['feed_events'] > 0
    assert batch.publish_13f_batch(db, identifier_documents=[], retry_waiting=True)['processed'] == 0


def test_peer_transport_drift_aborts_before_any_publication(db):
    prior, current = pair()
    stage(db, prior)
    stage(db, current)
    revision = db.scalar(select(DirectFeedRevision))
    revision.source_bytes += b'changed'
    db.commit()
    with pytest.raises(ValueError, match='checksum'):
        batch.publish_13f_batch(db, identifier_documents=[])
    db.rollback()
    assert db.scalar(select(func.count()).select_from(DirectFeedPublication)) == 0
    assert db.scalar(select(func.count()).select_from(Event)) == 0


def test_held_receipt_cannot_starve_new_documents(db):
    prior, current = pair()
    doc_id = stage(db, prior)
    doc = db.get(DirectFeedDocument, doc_id)
    doc.metadata_json = json.dumps({**json.loads(doc.metadata_json), 'filing_date': '2099-01-01'})
    db.commit()
    stage(db, current)
    first = batch.publish_13f_batch(db, identifier_documents=[], limit=1)
    assert first['results'][0]['status'] == 'held'
    second = batch.publish_13f_batch(db, identifier_documents=[], limit=1)
    assert second['processed'] == 1 and second['results'][0]['document_id'] != doc_id


@pytest.mark.parametrize('tamper', ['checksum', 'escape', 'duplicate'])
def test_manifest_rejects_changed_or_unbounded_paths(tmp_path, tamper):
    folder = tmp_path / 'evidence'
    folder.mkdir()
    raw = b'reviewed'
    (folder / 'one.source').write_bytes(raw)
    entry = {'source_file': 'one.source', 'content_hash': hashlib.sha256(raw).hexdigest(), 'url': 'https://www.sec.gov/fixture'}
    entries = [entry]
    if tamper == 'checksum':
        (folder / 'one.source').write_bytes(b'changed')
    elif tamper == 'escape':
        (tmp_path / 'outside.source').write_bytes(raw)
        entry['source_file'] = '../outside.source'
    else:
        entries.append(dict(entry))
    manifest = folder / 'manifest.json'
    manifest.write_text(json.dumps({'documents': entries}))
    with pytest.raises(ValueError):
        batch.load_identifier_manifest(manifest)


def test_disabled_job_never_opens_manifest_or_database(monkeypatch, capsys):
    from app.jobs import publish_direct_13fs as job
    monkeypatch.delenv('DIRECT_13F_PUBLICATION_ENABLED', raising=False)
    monkeypatch.setattr('sys.argv', ['publisher'])
    monkeypatch.setattr(job, 'SessionLocal', lambda: pytest.fail('Opened DB'))
    monkeypatch.setattr(job, 'load_identifier_manifest', lambda *a: pytest.fail('Opened manifest'))
    job.main()
    assert json.loads(capsys.readouterr().out)['status'] == 'disabled'
