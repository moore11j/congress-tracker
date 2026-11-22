"""Explicit, expiring SEC13F collection hold for a consistent original copy.

This does not pause quotes, alerts, publication or other source collection. The
operator creates/removes the file while holding the existing collector lock;
collectors consult it inside that same lock before changing SEC13F originals.
"""
import json
import math
import os
from pathlib import Path
import re
import time

from app.services.institutional_capture_transport import decode

WINDOW_PATH = '/data/institutional-original-copy-window.json'
MAX_SECONDS = 3600
SCOPE = 'sec_13f_originals_and_prior_processing'


def validate_window(value):
    if (not isinstance(value, dict) or set(value) != {
            'version', 'window_id', 'package_sha256', 'approved_at', 'expires_at', 'scope'} or
            type(value['version']) is not int or value['version'] != 1 or
            not isinstance(value['window_id'], str) or not re.fullmatch(r'[a-f0-9]{32}', value['window_id']) or
            not isinstance(value['package_sha256'], str) or not re.fullmatch(r'[a-f0-9]{64}', value['package_sha256']) or
            value['scope'] != SCOPE or
            any(type(value[k]) not in (int, float) or not math.isfinite(value[k])
                for k in ('approved_at', 'expires_at')) or
            not 0 < value['expires_at'] - value['approved_at'] <= MAX_SECONDS):
        raise ValueError('invalid_original_copy_window')
    return value


def read_window(path=WINDOW_PATH, *, now=None, expected_package=None):
    path = Path(path)
    if not path.exists() and not path.is_symlink():
        return None
    if path.is_symlink() or not path.is_file() or path.stat().st_size > 2048:
        raise ValueError('invalid_original_copy_window_file')
    value = validate_window(decode(path.read_bytes()))
    now = time.time() if now is None else now
    # An expired lease never suppresses a normal collection or renews itself.
    if now >= value['expires_at']:
        return None
    if now < value['approved_at']:
        raise ValueError('future_original_copy_window')
    if expected_package is None:
        from app.jobs.capture_direct_13f_evidence import package_hash
        expected_package = package_hash()
    if value['package_sha256'] != expected_package:
        raise ValueError('original_copy_window_package_changed')
    return value


def create_window(value, path=WINDOW_PATH, *, now=None, expected_package):
    """Caller must hold collector_lock. Existing files, even expired, are never renewed."""
    validate_window(value)
    now = time.time() if now is None else now
    if not value['approved_at'] <= now < value['expires_at'] or value['package_sha256'] != expected_package:
        raise ValueError('unapproved_original_copy_window')
    path = Path(path)
    raw = json.dumps(value, sort_keys=True, separators=(',', ':')).encode()
    with path.open('xb') as output:
        os.chmod(path, 0o600)
        output.write(raw)
        output.flush()
        os.fsync(output.fileno())
    return value


def close_window(window_id, path=WINDOW_PATH):
    """Caller must hold collector_lock; preserve the exact consumed approval."""
    path = Path(path)
    if path.is_symlink() or not path.is_file() or path.stat().st_size > 2048:
        raise ValueError('invalid_original_copy_window_file')
    value = validate_window(decode(path.read_bytes()))
    if value['window_id'] != window_id:
        raise ValueError('original_copy_window_not_owned')
    archive = path.with_name('institutional-original-copy-' + window_id + '.closed.json')
    os.link(path, archive)
    path.unlink()
    return value
