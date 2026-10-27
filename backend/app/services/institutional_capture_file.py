"""Local capture-file preparation. No database, scheduler or network access.

A completed file is transport evidence only. The original-scope receiver must
still validate it. Callers must provide scheduler ownership and budget checks.
"""
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import shutil

MAX_COMPRESSED_BYTES = 250_000_000
MIN_FREE_BYTES = 600_000_000


class BoundedWriter:
    def __init__(self, raw, checkpoint, limit=MAX_COMPRESSED_BYTES):
        self.raw = raw
        self.checkpoint = checkpoint
        self.limit = limit
        self.size = 0
        self.digest = hashlib.sha256()

    def write(self, data):
        self.checkpoint()
        if self.size + len(data) > self.limit:
            raise ValueError('compressed_file_bound')
        written = self.raw.write(data)
        if written != len(data):
            raise OSError('short_capture_write')
        self.digest.update(data)
        self.size += written
        return written

    def flush(self):
        self.raw.flush()


def paths(root, cycle):
    if not isinstance(cycle, str) or not re.fullmatch(r'[a-f0-9]{32}', cycle):
        raise ValueError('invalid_capture_cycle')
    root = Path(root)
    if root.is_symlink() or not root.is_dir():
        raise ValueError('capture_root_not_directory')
    root = root.resolve(strict=True)
    return root / (cycle + '.jsonl.gz'), root / (cycle + '.complete.json')


def prepare_file(root, cycle, producer, checkpoint, *, limit=MAX_COMPRESSED_BYTES):
    """Never overwrite or silently restart an existing cycle, even if partial.

    The producer owns its read-only transaction and must close it before returning
    a terminal stream receipt. Completion is published only after gzip closes and
    the payload is flushed. Failed artifacts remain visibly incomplete for an
    explicit owned-file cleanup, never an automatic retry of production work.
    """
    if type(limit) is not int or not 1 <= limit <= MAX_COMPRESSED_BYTES:
        raise ValueError('invalid_compressed_bound')
    payload, manifest = paths(root, cycle)
    checkpoint()
    if payload.exists() or manifest.exists():
        raise FileExistsError('capture_cycle_already_exists')
    if shutil.disk_usage(payload.parent).free < MIN_FREE_BYTES:
        raise RuntimeError('capture_disk_headroom')
    # O_EXCL also rejects existing dangling symlinks. No path is reused on failure.
    with payload.open('xb') as raw:
        writer = BoundedWriter(raw, checkpoint, limit)
        with gzip.GzipFile(fileobj=writer, mode='wb', filename='', mtime=0, compresslevel=1) as archive:
            receipt = producer(archive)
            if not isinstance(receipt, dict) or receipt.get('transaction_read_only') is not True:
                raise ValueError('missing_readonly_receipt')
            if type(receipt.get('production_writes')) is not int or receipt['production_writes'] != 0:
                raise ValueError('nonzero_production_writes')
        raw.flush()
        os.fsync(raw.fileno())
    checkpoint()
    result = dict(cycle=cycle, filename=payload.name, compressed_bytes=writer.size,
                  compressed_sha256=writer.digest.hexdigest(), stream_receipt=receipt,
                  acceptance='transport_only_requires_original_scope_verification')
    from app.services.institutional_capture_transport import encode, decode
    encoded = encode(result)
    if len(encoded) > 2_000_000:
        raise ValueError('manifest_size_bound')
    result = decode(encoded)
    pending = manifest.with_suffix('.pending')
    with pending.open('xb') as output:
        output.write(encoded)
        output.flush()
        os.fsync(output.fileno())
    # Link creation is exclusive and atomic: incomplete JSON is never published.
    os.link(pending, manifest)
    pending.unlink()
    return result


def verify_file(root, cycle):
    """Bounded local transfer integrity; never treats this as source acceptance."""
    payload, manifest = paths(root, cycle)
    if payload.is_symlink() or manifest.is_symlink():
        raise ValueError('capture_symlink')
    if manifest.stat().st_size > 2_000_000:
        raise ValueError('manifest_size_bound')
    from app.services.institutional_capture_transport import decode
    result = decode(manifest.read_bytes())
    if (not isinstance(result, dict) or result.get('cycle') != cycle or
            result.get('filename') != payload.name or
            type(result.get('compressed_bytes')) is not int or
            not 1 <= result['compressed_bytes'] <= MAX_COMPRESSED_BYTES or
            result.get('acceptance') != 'transport_only_requires_original_scope_verification'):
        raise ValueError('invalid_capture_manifest')
    if payload.stat().st_size != result['compressed_bytes']:
        raise ValueError('capture_size_mismatch')
    digest = hashlib.sha256()
    count = 0
    with payload.open('rb') as source:
        while chunk := source.read(1024 * 1024):
            count += len(chunk)
            if count > result['compressed_bytes']:
                raise ValueError('capture_grew_during_read')
            digest.update(chunk)
    if count != result['compressed_bytes'] or digest.hexdigest() != result.get('compressed_sha256'):
        raise ValueError('capture_checksum_mismatch')
    return result
