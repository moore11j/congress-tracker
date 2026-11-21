"""Local-only assembly of all approved original pieces, never a canonical baseline."""
import gzip
import os
from pathlib import Path
import shutil

from app.services.institutional_capture_file import BoundedWriter, verify_file
from app.services.institutional_capture_transport import StreamReceiver, decode, encode
from app.services.institutional_original_segments import ACCEPTANCE, validate_plan


def receive_plan(source_root, plan, cycles, destination, parser=None):
    validate_plan(plan)
    if (not isinstance(cycles, list) or len(cycles) != len(plan['pieces']) or
            any(not isinstance(value, str) for value in cycles) or len(set(cycles)) != len(cycles)):
        raise ValueError('one_unique_capture_cycle_per_piece_required')
    # Validate all names and transfer hashes before creating a destination.
    manifests = [verify_file(source_root, cycle) for cycle in cycles]
    destination = Path(destination)
    if shutil.disk_usage(destination.parent).free < 12_000_000_000:
        raise ValueError('local_original_copy_disk_headroom')
    destination.mkdir()  # Existing complete or incomplete cycles are never reused.
    pending = destination / 'originals.pending.jsonl.gz'
    ciks, cusips, periods, rows, verified = set(), set(), set(), 0, 0
    receipts = []
    with pending.open('xb') as raw:
        bounded = BoundedWriter(raw, lambda: None, limit=6_000_000_000)
        with gzip.GzipFile(fileobj=bounded, mode='wb', filename='', mtime=0, compresslevel=1) as archive:
            class DocumentsOnly:
                def write(self, line):
                    nonlocal rows
                    item = decode(line)
                    if item['table'] == 'documents':
                        archive.write(line)
                        rows += 1

            for piece, cycle, manifest in zip(plan['pieces'], cycles, manifests, strict=True):
                reader = StreamReceiver(DocumentsOnly(), parser, original_piece=piece)
                with gzip.open(Path(source_root) / manifest['filename'], 'rb') as source:
                    reader.read(source)
                report = reader.finish(0)
                if reader.receipt != manifest['stream_receipt']:
                    raise ValueError('transferred_manifest_receipt_mismatch')
                ciks.update(reader.required_ciks)
                cusips.update(reader.required_cusips)
                periods.update(reader.required_periods)
                verified += reader.originals_verified
                receipts.append(dict(index=piece['index'], cycle=cycle,
                    compressed_sha256=manifest['compressed_sha256'], stream_sha256=report['sha256']))
        raw.flush()
        os.fsync(raw.fileno())
    if rows != plan['document_count']:
        raise ValueError('complete_inventory_count_mismatch')
    result = dict(acceptance=ACCEPTANCE, inventory_sha256=plan['inventory_sha256'],
        document_count=rows, originals_verified=verified, source_bytes=plan['source_bytes'],
        compressed_bytes=bounded.size, compressed_sha256=bounded.digest.hexdigest(),
        query_scope=dict(ciks=sorted(ciks), cusips=sorted(cusips), periods=[list(p) for p in sorted(periods)]),
        pieces=receipts, production_writes=0, emails=0, external_requests=0)
    # Receipt publication is last. A partial assembly is never an accepted source set.
    target = destination / 'originals.jsonl.gz'
    os.link(pending, target)
    pending.unlink()
    receipt_pending = destination / 'originals.receipt.pending.json'
    with receipt_pending.open('xb') as output:
        output.write(encode(result))
        output.flush()
        os.fsync(output.fileno())
    os.link(receipt_pending, destination / 'originals.verified.json')
    receipt_pending.unlink()
    return result
