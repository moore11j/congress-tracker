import gzip
import io
import json
from contextlib import nullcontext
from datetime import datetime, timezone
from unittest.mock import patch

import pytest

from app.services import institutional_capture_file as files
from app.services.institutional_capture_transport import StreamReceiver
from test_institutional_capture_transport import parser, stream

CYCLE = 'a' * 32


def producer(output):
    lines = stream()
    for line in lines:
        output.write(line)
    return json.loads(lines[-1])['complete']


def test_complete_file_requires_and_passes_original_scope_receiver(tmp_path):
    calls = []
    result = files.prepare_file(tmp_path, CYCLE, producer, lambda: calls.append(True))
    assert calls and files.verify_file(tmp_path, CYCLE) == result
    receiver = StreamReceiver(io.BytesIO(), parser)
    with gzip.open(tmp_path / result['filename'], 'rb') as source:
        for line in source:
            receiver.accept(line)
    assert receiver.finish(0)['scope_verified']


def test_actual_database_receipt_timestamp_is_serialized_without_losing_values(tmp_path):
    captured_at = datetime(2026, 10, 27, 7, 0, tzinfo=timezone.utc)
    def dated(output):
        return {**producer(output), 'captured_at': captured_at}
    result = files.prepare_file(tmp_path, CYCLE, dated, lambda: None)
    stored = files.verify_file(tmp_path, CYCLE)
    assert stored['stream_receipt']['captured_at'] == str(captured_at)
    assert stored['compressed_sha256'] == result['compressed_sha256']


def test_compressed_overflow_leaves_no_completion_and_never_restarts(tmp_path):
    with pytest.raises(ValueError, match='compressed_file_bound'):
        files.prepare_file(tmp_path, CYCLE, producer, lambda: None, limit=12)
    assert not (tmp_path / (CYCLE + '.complete.json')).exists()
    with pytest.raises(FileExistsError):
        files.prepare_file(tmp_path, CYCLE, producer, lambda: None)


@pytest.mark.parametrize('phase', ['producer', 'close', 'final_checkpoint', 'fsync'])
def test_interruption_never_creates_completion(tmp_path, phase):
    def fail(*args):
        raise RuntimeError('injected_failure')
    original_close = files.gzip.GzipFile.close
    def fail_close(archive):
        was_open = archive.fileobj is not None
        original_close(archive)
        if was_open:
            fail()
    def checkpoint():
        if phase == 'final_checkpoint' and (tmp_path / (CYCLE + '.jsonl.gz')).exists():
            # Gzip trailer size write identifies the fully closed transport.
            data = (tmp_path / (CYCLE + '.jsonl.gz')).read_bytes()
            if len(data) > 50:
                fail()
    with patch.object(files.os, 'fsync', side_effect=fail if phase == 'fsync' else None):
        with patch.object(files.gzip.GzipFile, 'close', new=fail_close) if phase == 'close' else nullcontext():
            with pytest.raises(RuntimeError, match='injected_failure'):
                files.prepare_file(tmp_path, CYCLE, fail if phase == 'producer' else producer, checkpoint)
    assert not (tmp_path / (CYCLE + '.complete.json')).exists()


@pytest.mark.parametrize('mutation', ['truncate', 'same_size', 'manifest_cycle', 'manifest_size', 'manifest_duplicate'])
def test_transfer_mutations_are_rejected(tmp_path, mutation):
    result = files.prepare_file(tmp_path, CYCLE, producer, lambda: None)
    payload, manifest = files.paths(tmp_path, CYCLE)
    if mutation == 'truncate':
        payload.write_bytes(payload.read_bytes()[:-1])
    elif mutation == 'same_size':
        raw = payload.read_bytes(); payload.write_bytes(b'!' + raw[1:])
    elif mutation == 'manifest_duplicate':
        manifest.write_text('{"cycle":"x","cycle":"y"}')
    else:
        result['cycle' if mutation == 'manifest_cycle' else 'compressed_bytes'] = 'b' * 32 if mutation == 'manifest_cycle' else True
        manifest.write_text(json.dumps(result))
    with pytest.raises(ValueError):
        files.verify_file(tmp_path, CYCLE)


@pytest.mark.parametrize('cycle', ['../escape', 'A' * 32, '', 'a' * 31, None])
def test_invalid_cycle_cannot_create_any_file(tmp_path, cycle):
    with pytest.raises(ValueError):
        files.prepare_file(tmp_path, cycle, producer, lambda: None)
    assert not list(tmp_path.iterdir())


def test_insufficient_disk_fails_before_producer(tmp_path):
    with patch.object(files.shutil, 'disk_usage', return_value=type('Disk', (), {'free': 1})()):
        with pytest.raises(RuntimeError, match='headroom'):
            files.prepare_file(tmp_path, CYCLE, lambda _: pytest.fail('producer called'), lambda: None)
    assert not list(tmp_path.iterdir())


def test_queue_guard_rejection_prevents_output(tmp_path):
    def deny():
        raise RuntimeError('not_lane_owner')
    with pytest.raises(RuntimeError, match='not_lane_owner'):
        files.prepare_file(tmp_path, CYCLE, producer, deny)
    assert not list(tmp_path.iterdir())
