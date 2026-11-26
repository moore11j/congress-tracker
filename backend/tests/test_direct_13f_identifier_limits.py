import json
import pytest
from sqlalchemy import select, func

from app.clients.direct_sources import DirectSourceError
from app.services import direct_13f_identifier_limits as limits
from app.services.direct_13f_batch import load_identifier_manifest
from app.services.direct_13f_identifiers import stage_reviewed_identifiers, load_staged_identifiers, reviewed_entries
from app.services.direct_feed_store import DirectFeedDocument
from test_direct_sec_collection import db
from test_direct_13f_evidence import nport


def setup_source(tmp_path):
    source = nport()
    (tmp_path / 'original.source').write_bytes(source['raw'])
    manifest = tmp_path / 'manifest.json'
    manifest.write_text(json.dumps({'documents': [dict(source_file='original.source',
        url=source['url'], content_hash=source['content_hash'])]}))
    return source, manifest


def test_exact_shared_size_boundary_passes_all_three_interfaces(db, tmp_path, monkeypatch):
    source, manifest = setup_source(tmp_path)
    monkeypatch.setattr(limits, 'MAX_SOURCE_BYTES', len(source['raw']))
    monkeypatch.setattr(limits, 'MAX_CORPUS_BYTES', len(source['raw']))
    assert load_identifier_manifest(manifest) == [source]
    class Client:
        def get(self, url): return source['raw']
    assert stage_reviewed_identifiers(db, Client(), manifest)['public_writes'] == 0
    assert load_staged_identifiers(db, manifest) == [source]


@pytest.mark.parametrize('bound', ['MAX_SOURCE_BYTES', 'MAX_CORPUS_BYTES'])
def test_staging_and_file_reader_reject_one_byte_over_shared_bound(db, tmp_path, monkeypatch, bound):
    source, manifest = setup_source(tmp_path)
    monkeypatch.setattr(limits, bound, len(source['raw']) - 1)
    with pytest.raises(ValueError, match='byte limits|size limit'):
        load_identifier_manifest(manifest)
    class Client:
        def get(self, url): return source['raw']
    with pytest.raises(DirectSourceError, match='byte limits'):
        stage_reviewed_identifiers(db, Client(), manifest)
    assert db.scalar(select(func.count()).select_from(DirectFeedDocument)) == 0


@pytest.mark.parametrize('bound', ['MAX_SOURCE_BYTES', 'MAX_CORPUS_BYTES'])
def test_saved_reader_still_rejects_oversized_originals(db, tmp_path, monkeypatch, bound):
    source, manifest = setup_source(tmp_path)
    class Client:
        def get(self, url): return source['raw']
    stage_reviewed_identifiers(db, Client(), manifest)
    monkeypatch.setattr(limits, bound, len(source['raw']) - 1)
    with pytest.raises(DirectSourceError, match='byte limits'):
        load_staged_identifiers(db, manifest)


@pytest.mark.parametrize('count', [0, 51])
def test_document_count_bound_remains_unchanged(tmp_path, count):
    _, manifest = setup_source(tmp_path)
    value = json.loads(manifest.read_bytes())
    value['documents'] *= count
    manifest.write_text(json.dumps(value))
    with pytest.raises(ValueError, match='one to fifty'):
        reviewed_entries(manifest)
    with pytest.raises(ValueError, match='1 to 50'):
        load_identifier_manifest(manifest)
