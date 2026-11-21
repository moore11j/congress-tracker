"""Verify a complete frozen source inventory locally; no production connections."""
import argparse
import os
from pathlib import Path
import socket
import sys

os.environ['DATABASE_URL'] = 'sqlite://'
def denied(*args, **kwargs):
    raise RuntimeError('Original verification is offline only')
socket.socket.connect = denied
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app.services.institutional_capture_transport import decode, encode
from app.services.institutional_original_receive import receive_plan


def read_json(path, maximum):
    path = Path(path)
    if path.is_symlink() or path.stat().st_size > maximum:
        raise ValueError('local_manifest_bound')
    return decode(path.read_bytes())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-root', required=True)
    parser.add_argument('--plan', required=True)
    parser.add_argument('--cycles', required=True, help='JSON list of capture IDs in plan order')
    parser.add_argument('--destination', required=True)
    args = parser.parse_args()
    report = receive_plan(args.source_root, read_json(args.plan, 8_000_000),
        read_json(args.cycles, 8192), args.destination)
    print(encode({k: report[k] for k in ('acceptance', 'document_count', 'originals_verified',
        'source_bytes', 'inventory_sha256', 'production_writes', 'emails', 'external_requests')}).decode())


if __name__ == '__main__':
    main()
