import io
import json
from types import SimpleNamespace

import pytest

from app.services import institutional_original_segments as pieces
from app.services.institutional_capture_transport import RecordStream, StreamReceiver
from app.services.institutional_capture_file import prepare_file
from app.services.institutional_original_receive import receive_plan
from app.jobs import capture_direct_13f_evidence as job
from test_institutional_capture_transport import RAW, DOC, SCOPE, parser


def row(ident=1, size=len(RAW), metadata=100):
    return [ident, DOC['content_hash'], 'parsed', f'123:({ident},1)', ident,
            f'124:({ident},1)', size, metadata, 100]


def plan():
    return pieces.make_plan([row()], snapshot_at=100)


def wire(piece=None, *, changed=None, canonical=False):
    piece = piece or plan()['pieces'][0]
    output = io.BytesIO()
    writer = RecordStream(output)
    writer.write('metadata', dict(transport_version=3, transaction_read_only=True,
        filing_date='2026-10-07', document_count=1, original_piece=piece, acceptance=pieces.ACCEPTANCE))
    doc = {k: v for k, v in DOC.items() if k != 'raw_base64'}
    if changed:
        doc.update(changed)
    writer.write_document(doc, RAW)
    if canonical:
        writer.write('positions', {'id': 22})
    receipt = writer.finish(transaction_read_only=True, production_writes=0,
        source_bytes=len(RAW), query_scope=SCOPE, original_piece_sha256=pieces.digest(piece),
        acceptance=pieces.ACCEPTANCE)
    return output.getvalue(), receipt


def test_large_inventory_is_complete_but_individually_bounded():
    rows = [row(i, size=220_000) for i in range(1, 17453)]
    result = pieces.make_plan(rows)
    assert result['source_bytes'] == 17452 * 220_000 > 1_000_000_000
    assert pieces.validate_plan(result) == result
    assert [r for p in result['pieces'] for r in p['rows']] == rows
    assert all(len(p['rows']) <= 250 and sum(pieces.estimated_bytes(r) for r in p['rows']) <= 64_000_000
               for p in result['pieces'])


@pytest.mark.parametrize('change', ['duplicate', 'order', 'missing', 'hash', 'bool', 'bytes', 'too_many', 'total_bytes', 'piece', 'metadata', 'parsed_missing'])
def test_omission_drift_and_bounds_reject_instead_of_truncating(change):
    result = pieces.make_plan([row(1), row(2)])
    if change == 'duplicate': result['pieces'][0]['rows'].append(row(2))
    elif change == 'order': result['pieces'][0]['rows'].reverse()
    elif change == 'missing': result['pieces'][0]['rows'].pop()
    elif change == 'hash': result['inventory_sha256'] = '0' * 64
    elif change == 'bool': result['pieces'][0]['index'] = False
    elif change == 'bytes': result['pieces'][0]['rows'][0][6] = 30_000_001
    elif change == 'too_many': result['pieces'][0]['rows'] = [row(i) for i in range(1, 252)]
    elif change == 'total_bytes':
        with pytest.raises(ValueError, match='source_byte_bound'):
            pieces.make_plan([row(i, 30_000_000) for i in range(1, 135)])
        return
    elif change == 'piece': result['pieces'][0]['total'] = 129
    elif change == 'metadata': result['pieces'][0]['rows'][0][7] = 11_000_000
    elif change == 'parsed_missing': result['pieces'][0]['rows'][0][6] = 0
    with pytest.raises(ValueError):
        pieces.validate_plan(result)


def test_missing_failed_original_is_preserved_in_inventory():
    failed = [2, None, 'failed', '125:(2,1)', None, None, 0, 100, 0]
    result = pieces.make_plan([row(), failed])
    assert result['document_count'] == 2 and result['pieces'][0]['rows'][1] == failed


def test_complete_piece_reparses_and_cannot_be_mistaken_for_canonical_baseline():
    raw, receipt = wire()
    reader = StreamReceiver(io.BytesIO(), parser, original_piece=plan()['pieces'][0])
    reader.read(io.BytesIO(raw))
    assert reader.finish(0)['originals_verified'] == 1
    assert reader.receipt == receipt
    with pytest.raises(ValueError, match='not canonical baselines'):
        StreamReceiver(io.BytesIO(), parser).read(io.BytesIO(raw))


@pytest.mark.parametrize('change', ['identity', 'status', 'hash', 'canonical', 'truncated', 'piece'])
def test_piece_stream_tampering_rejected(change):
    options = {}
    if change == 'identity': options['changed'] = {'id': 2}
    elif change == 'status': options['changed'] = {'status': 'quarantined'}
    elif change == 'hash': options['changed'] = {'content_hash': '0' * 64}
    elif change == 'canonical': options['canonical'] = True
    raw, _ = wire(**options)
    expected = plan()['pieces'][0]
    if change == 'piece': expected = {**expected, 'index': 1, 'total': 2}
    if change == 'truncated': raw = raw[:-25]
    reader = StreamReceiver(io.BytesIO(), parser, original_piece=expected)
    with pytest.raises(ValueError):
        reader.read(io.BytesIO(raw))
        reader.finish(0)


def test_source_copy_approval_is_distinct_from_legacy_capture(tmp_path):
    value = dict(version=2, request_id='a' * 32, package_sha256=job.package_hash(),
        approved_at=100, expires_at=200, payload='institutional_originals_segment',
        destination='owner_local_onedrive_migration_validation', original_piece=plan()['pieces'][0])
    path = tmp_path / 'request.json'
    path.write_text(json.dumps(value))
    assert job.read_request(path, now=150) == value
    for changes in ({'version': 1}, {'payload': 'institutional_originals_and_canonical_reference_rows'},
                    {'original_piece': None}, {'expires_at': 150}, {'package_sha256': '0' * 64}):
        path.write_text(json.dumps({**value, **changes}))
        with pytest.raises(ValueError):
            job.read_request(path, now=150)


def test_local_complete_assembly_publishes_only_originals_receipt(tmp_path, monkeypatch):
    import gzip
    import app.services.institutional_original_receive as receiver
    monkeypatch.setattr(receiver.shutil, 'disk_usage', lambda _: SimpleNamespace(free=20_000_000_000))
    raw, receipt = wire()
    def producer(output):
        output.write(raw)
        return receipt
    prepare_file(tmp_path, 'a' * 32, producer, lambda: None)
    target = tmp_path / 'verified'
    result = receive_plan(tmp_path, plan(), ['a' * 32], target, parser)
    assert result['acceptance'] == pieces.ACCEPTANCE and result['originals_verified'] == 1
    assert result['query_scope'] == SCOPE
    with gzip.open(target / 'originals.jsonl.gz', 'rb') as source:
        items = [json.loads(line) for line in source]
    assert items == [{'table': 'documents', 'row': DOC}]
    assert (target / 'originals.verified.json').exists()
    assert not list(target.glob('*.pending*'))
    assert not list(target.glob('*baseline*'))
    with pytest.raises(FileExistsError):
        receive_plan(tmp_path, plan(), ['a' * 32], target, parser)


def test_changed_source_tokens_fail_before_any_original_is_read(monkeypatch):
    class DB:
        def execute(self, *args, **kwargs):
            pytest.fail('Originals must not be read after inventory drift')
    monkeypatch.setattr(pieces, 'snapshot_rows', lambda *args: [row(2)])
    output = io.BytesIO()
    with pytest.raises(ValueError, match='source_inventory_changed'):
        pieces.export_piece(DB(), RecordStream(output), plan()['pieces'][0])
    assert output.getvalue() == b''


def test_terminal_scope_cannot_exceed_the_whole_piece_bound(monkeypatch):
    writer = RecordStream(io.BytesIO())
    writer.write('metadata', dict(transport_version=3))
    monkeypatch.setattr(pieces, 'PIECE_BYTES', 10)
    with pytest.raises(ValueError, match='complete_piece_byte_bound'):
        pieces.finish_piece(writer, dict(query_scope={'cusips': ['123456789']}))
    assert writer.done is False


@pytest.mark.parametrize('broken', [False, True])
def test_whole_inventory_assembly_requires_every_piece(tmp_path, monkeypatch, broken):
    import app.services.institutional_original_receive as receiver
    monkeypatch.setattr(receiver.shutil, 'disk_usage', lambda _: SimpleNamespace(free=20_000_000_000))
    monkeypatch.setattr(pieces, 'PIECE_DOCUMENTS', 1)
    complete = pieces.make_plan([row(1), row(2)], snapshot_at=100)
    cycles = ['a' * 32, 'b' * 32]
    for piece, cycle in zip(complete['pieces'], cycles, strict=True):
        raw, receipt = wire(piece, changed={'id': 1 if broken else piece['rows'][0][0]})
        def producer(output, raw=raw, receipt=receipt):
            output.write(raw)
            return receipt
        prepare_file(tmp_path, cycle, producer, lambda: None)
    with pytest.raises(ValueError, match='unique_capture_cycle'):
        receive_plan(tmp_path, complete, cycles[:1], tmp_path / 'missing', parser)
    assert not (tmp_path / 'missing').exists()
    target = tmp_path / 'assembled'
    if broken:
        with pytest.raises(ValueError, match='identity mismatch'):
            receive_plan(tmp_path, complete, cycles, target, parser)
        assert not (target / 'originals.verified.json').exists()
        assert (target / 'originals.pending.jsonl.gz').exists()
    else:
        result = receive_plan(tmp_path, complete, cycles, target, parser)
        assert result['document_count'] == result['originals_verified'] == 2


@pytest.mark.parametrize('snapshot_at', [-1, 151, 0])
def test_inventory_time_cannot_bypass_approval_window(tmp_path, snapshot_at):
    piece = {**plan()['pieces'][0], 'snapshot_at': snapshot_at}
    value = dict(version=2, request_id='a' * 32, package_sha256=job.package_hash(),
        approved_at=100, expires_at=5000, payload='institutional_originals_segment',
        destination='owner_local_onedrive_migration_validation', original_piece=piece)
    # Keep the approval itself valid; vary only the inventory age/future time.
    now = 4000 if snapshot_at == 0 else 150
    value.update(approved_at=now-1, expires_at=now+300)
    path = tmp_path / 'request.json'
    path.write_text(json.dumps(value))
    with pytest.raises(ValueError):
        job.read_request(path, now=now)
