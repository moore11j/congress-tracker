import json

import pytest
from sqlalchemy import select, func

from app.clients.direct_sources import DirectSourceError
from app.services.direct_13f_identifiers import stage_reviewed_identifiers, load_staged_identifiers, reviewed_entries
from app.services.direct_feed_store import DirectFeedDocument, DirectFeedRevision
from test_direct_sec_collection import db
from test_direct_13f_evidence import nport


def manifest(tmp_path, source):
    path = tmp_path / 'manifest.json'
    path.write_text(json.dumps({'documents': [{k: source[k] for k in ('url', 'content_hash')}]}))
    return path


def test_shared_staging_reuses_original_evidence_and_rejects_drift(db, tmp_path):
    source = nport(); path = manifest(tmp_path, source); calls = []
    class Client:
        def get(self, url):
            assert not db.in_transaction(); calls.append(url); return source['raw']
    first = stage_reviewed_identifiers(db, Client(), path)
    assert first['documents'][0]['reused'] is False
    assert load_staged_identifiers(db, path) == [source]
    repeat = stage_reviewed_identifiers(db, Client(), path)
    assert repeat['documents'][0]['reused'] is True and len(calls) == 1
    assert db.scalar(select(func.count()).select_from(DirectFeedRevision)) == 1
    revision = db.scalar(select(DirectFeedRevision)); revision.source_bytes += b'changed'; db.commit()
    with pytest.raises(DirectSourceError, match='checksum'): load_staged_identifiers(db, path)


def test_reviewed_checksum_is_required_before_staging(db, tmp_path):
    source = nport(); path = manifest(tmp_path, source)
    class Client:
        def get(self, url): return source['raw'] + b'changed'
    with pytest.raises(DirectSourceError, match='checksum'): stage_reviewed_identifiers(db, Client(), path)
    assert db.scalar(select(func.count()).select_from(DirectFeedDocument)) == 0


@pytest.mark.parametrize('kind', ['host', 'duplicate', 'hash'])
def test_manifest_identity_validation(tmp_path, kind):
    source = nport(); path = manifest(tmp_path, source)
    data = json.loads(path.read_text())
    if kind == 'host': data['documents'][0]['url'] = source['url'].replace('www.sec.gov', 'example.com')
    elif kind == 'hash': data['documents'][0]['content_hash'] = 'bad'
    else: data['documents'] *= 2
    path.write_text(json.dumps(data))
    with pytest.raises(ValueError): reviewed_entries(path)
