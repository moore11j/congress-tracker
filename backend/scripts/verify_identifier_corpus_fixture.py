"""Offline resource and repeat proof using all thirteen complete SEC originals."""
from pathlib import Path
from datetime import date
import argparse
import gzip
import hashlib
import json
import os
import socket
import sys
import time

parser = argparse.ArgumentParser()
parser.add_argument('--work-dir', type=Path, required=True)
args = parser.parse_args()
work = args.work_dir.resolve()
work.mkdir(parents=True, exist_ok=False)
os.environ['DATABASE_URL'] = 'sqlite://'
def denied(*args, **kwargs):
    raise AssertionError('Identifier corpus proof attempted external transport')
socket.socket.connect = denied
started = time.monotonic()
if os.name == 'posix':
    import resource
    import signal
    resource.setrlimit(resource.RLIMIT_CPU, (45, 50))
    resource.setrlimit(resource.RLIMIT_AS, (1_000_000_000, 1_000_000_000))
    signal.alarm(70)
backend = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(backend))
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session
from app.db import Base
from app.services.direct_feed_store import DirectFeedDocument, DirectFeedRevision, dumps
from app.services.direct_13f_batch import load_identifier_manifest
from app.services.direct_13f_identifiers import stage_reviewed_identifiers, load_staged_identifiers
from app.services.direct_13f_evidence import nport_identifiers
from app.services.direct_13f_identifier_limits import MAX_CORPUS_BYTES, MAX_SOURCE_BYTES, MAX_DOCUMENTS

fixture = backend / 'tests/fixtures/sec_nport_corpus'
manifest = json.loads((fixture / 'manifest.json').read_bytes())
config = json.loads((backend / 'config/sec_13f_identifiers.json').read_bytes())
assert manifest['documents'] == config['documents']
assert len(manifest['documents']) == 13 and MAX_DOCUMENTS == 50
assert MAX_SOURCE_BYTES == 30_000_000 and MAX_CORPUS_BYTES == 60_000_000
for row in manifest['documents']:
    assert Path(row['source_file']).name == row['source_file']
    raw = gzip.decompress((fixture / (row['source_file'] + '.gz')).read_bytes())
    assert hashlib.sha256(raw).hexdigest() == row['content_hash']
    (work / row['source_file']).write_bytes(raw)
del raw
manifest_path = work / 'manifest.json'
manifest_path.write_text(json.dumps(manifest), encoding='utf-8')
documents = load_identifier_manifest(manifest_path)
assert sum(len(d['raw']) for d in documents) == manifest['complete_bytes'] == 52_115_168
expected = manifest['identifier_outputs']
assert set(expected) == {d['content_hash'] for d in documents}
for doc in documents:
    rows = nport_identifiers(doc, available_by=date.max)
    assert expected[doc['content_hash']] == hashlib.sha256(dumps(rows).encode()).hexdigest()
new = documents[-1]
assert not nport_identifiers(new, available_by=date(2026, 9, 27))
assert len(nport_identifiers(new, available_by=date(2026, 9, 28))) == 823
original_sources = {doc['url']: doc for doc in documents}
del documents, new, rows
calls = []
class Client:
    def get(self, url):
        calls.append(url)
        return original_sources[url]['raw']
engine = create_engine('sqlite:///' + (work / 'staging.sqlite').as_posix())
Base.metadata.create_all(engine, tables=[DirectFeedDocument.__table__, DirectFeedRevision.__table__])
def fingerprint(db):
    digest = hashlib.sha256()
    for model in (DirectFeedDocument, DirectFeedRevision):
        for row in db.execute(select(model.__table__).order_by(model.id)).mappings():
            value = dict(row)
            if 'source_bytes' in value:
                value['source_bytes_sha256'] = hashlib.sha256(value.pop('source_bytes')).hexdigest()
            if 'raw_text' in value and value['raw_text'] is not None:
                value['raw_text_sha256'] = hashlib.sha256(value.pop('raw_text').encode()).hexdigest()
            digest.update(dumps(value).encode() + b'\n')
    return digest.hexdigest()
try:
    with Session(engine) as db:
        first = stage_reviewed_identifiers(db, Client(), manifest_path)
        assert len(calls) == 13 and first['bytes'] == 52_115_168
        assert first['public_writes'] == 0 and all(not d['reused'] for d in first['documents'])
        before = fingerprint(db)
        repeat = stage_reviewed_identifiers(db, Client(), manifest_path)
        assert len(calls) == 13 and all(d['reused'] for d in repeat['documents'])
        saved = load_staged_identifiers(db, manifest_path)
        assert len(saved) == 13
        for doc in saved:
            assert hashlib.sha256(doc['raw']).hexdigest() == doc['content_hash']
            assert hashlib.sha256(dumps(nport_identifiers(doc, available_by=date.max)).encode()).hexdigest() == expected[doc['content_hash']]
        assert fingerprint(db) == before
        db.rollback()
finally:
    engine.dispose()
peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024 if os.name == 'posix' else None
assert peak is None or peak < 750 * 1024 * 1024
report = dict(status='complete_identifier_corpus_staging_and_repeat_verified', documents=13,
    original_bytes=52_115_168, corpus_limit=MAX_CORPUS_BYTES, previous_sources_unchanged=12,
    new_source_rows=823, source_outputs=expected, repeat_originals_and_metadata_identical=True,
    fixture_client_reads=len(calls), external_requests=0, production_writes=0, emails=0,
    elapsed_seconds=round(time.monotonic()-started, 3), peak_rss_bytes=peak)
(work / 'report.json').write_text(json.dumps(report, indent=2), encoding='utf-8')
print(json.dumps(report), flush=True)
