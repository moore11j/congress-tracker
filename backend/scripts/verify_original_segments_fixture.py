"""Offline installed-code proof with the complete retained 19 MB SEC original."""
from pathlib import Path
import gzip
import hashlib
import io
import json
import os
import socket
import sys
import time

os.environ['DATABASE_URL'] = 'sqlite://'
def denied(*args, **kwargs):
    raise AssertionError('Original-piece fixture attempted external transport')
socket.socket.connect = denied
if os.name == 'posix':
    import resource
    import signal
    resource.setrlimit(resource.RLIMIT_CPU, (15, 20))
    resource.setrlimit(resource.RLIMIT_AS, (1_000_000_000, 1_000_000_000))
    signal.alarm(22)
backend = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(backend))
from app.services.institutional_capture_transport import RecordStream, StreamReceiver
from app.services.institutional_original_segments import make_plan, digest, finish_piece, ACCEPTANCE
from app.services.direct_feed_collection import parse_document

fixture = backend / 'tests/fixtures/sec_13f_preparation_buffers'
source = json.loads((fixture / 'jpm-2026q2-metadata.json').read_bytes())
raw = gzip.decompress((fixture / 'jpm-2026q2.txt.gz').read_bytes())
assert hashlib.sha256(raw).hexdigest() == source['content_hash'] == '9691a80faaf95c1198a8ae84509fa49548c9f3d108203105154d23b141abdfc1'
_, parsed, reasons = parse_document('sec_13f', raw, source['metadata'])
assert not reasons
piece = make_plan([[1, source['content_hash'], 'parsed', '123:(1,1)', 1, '124:(1,1)',
    len(raw), len(json.dumps(source['metadata']).encode()), len(json.dumps(parsed).encode())]])['pieces'][0]
cusips = sorted({row['cusip'] for row in parsed['positions']})
del parsed
wire = io.BytesIO()
writer = RecordStream(wire)
writer.write('metadata', dict(transport_version=3, transaction_read_only=True, filing_date='2026-10-07',
    document_count=1, original_piece=piece, acceptance=ACCEPTANCE))
writer.write_document(dict(id=1, metadata=source['metadata'], content_hash=source['content_hash'], status='parsed'), raw)
finish_piece(writer, dict(transaction_read_only=True, production_writes=0, source_bytes=len(raw),
    query_scope=dict(ciks=[], cusips=cusips, periods=[]), original_piece_sha256=digest(piece), acceptance=ACCEPTANCE))
class Discard:
    def write(self, data):
        return len(data)
wire.seek(0)
reader = StreamReceiver(Discard(), original_piece=piece)
reader.read(wire)
result = reader.finish(0)
assert result['originals_verified'] == 1 and result['source_bytes'] == len(raw)
print(json.dumps(dict(original_sha256=source['content_hash'], original_bytes=len(raw),
    originals_verified=result['originals_verified'], complete_wire_bytes=len(wire.getbuffer()),
    production_writes=0, emails=0, external_requests=0, acceptance=ACCEPTANCE)))
