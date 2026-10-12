"""Rejected SEC originals remain auditable without hourly retry or publication."""
from datetime import date, datetime, timedelta, timezone
import hashlib
import json

import pytest
from sqlalchemy import func, select

from app.clients.direct_sources import DirectSourceError, Sec13FCoverMismatch
from app.models import Event, InstitutionalFiling, InstitutionalPosition, EmailDelivery
from app.services import direct_feed_collection as collector
from app.services.direct_feed_store import DirectFeedDocument, DirectFeedRevision
from test_direct_sec_collection import db, submission, META


class Source:
    def __init__(self, raw):
        self.raw = raw
        self.fetches = 0

    def get(self, url):
        if url.endswith('.idx'):
            return b'CIK|Company Name|Form Type|Date Filed|File Name\n1092903|Example|13F-HR|20261006|edgar/data/1092903/0001096906-26-001512.txt\n'
        self.fetches += 1
        if isinstance(self.raw, Exception):
            raise self.raw
        return self.raw


def collect(db, client):
    return collector.collect_direct_feeds(db, client, sources=['sec_13f'],
        start=date(2026, 10, 6), end=date(2026, 10, 6), recheck_hours=168)


def no_public_writes(db):
    for model in (Event, InstitutionalFiling, InstitutionalPosition, EmailDelivery):
        assert db.scalar(select(func.count()).select_from(model)) == 0


@pytest.mark.parametrize('old,new,field,value', [
    (b'<tableValueTotal>300', b'<tableValueTotal>301', 'declared_value_usd', '301'),
    (b'<tableEntryTotal>2', b'<tableEntryTotal>3', 'declared_count', 3),
])
def test_exact_mismatch_retains_bytes_and_review_cadence(db, monkeypatch, old, new, field, value):
    raw = submission().replace(old, new)
    with pytest.raises(Sec13FCoverMismatch):
        collector.parse_document('sec_13f', raw, META)
    client = Source(raw)
    def forbidden(*args, **kwargs):
        raise AssertionError('Rejected original must not enter canonical reconciliation')
    monkeypatch.setattr(collector, 'reconcile_institutional', forbidden)
    first = collect(db, client)
    assert first['status'] == 'partial' and not first['cutover_ready']
    assert first['results'] == {'quarantined': 1} and first['processed'] == 1
    doc = db.scalar(select(DirectFeedDocument))
    revision = db.scalar(select(DirectFeedRevision))
    assert doc.status == 'quarantined' and doc.attempts == 1
    assert revision.source_bytes == raw
    assert revision.content_hash == doc.content_hash == hashlib.sha256(raw).hexdigest()
    assert revision.source_text.encode() == raw
    parsed = json.loads(doc.parsed_json)
    assert parsed['positions'] == [] and parsed['validation']['status'] == 'held'
    assert parsed['validation'][field] == value
    assert parsed['validation']['actual_count'] == 2
    assert parsed['validation']['actual_value_usd'] == '300'
    assert doc.reconciliation_json is None
    # A two-hour-old transport failure would retry; this source defect must wait.
    doc.checked_at = datetime.now(timezone.utc) - timedelta(hours=2)
    db.commit()
    assert collect(db, client)['processed'] == 0
    assert client.fetches == 1
    doc.checked_at = datetime.now(timezone.utc) - timedelta(days=8)
    db.commit()
    assert collect(db, client)['results'] == {'quarantined': 1}
    assert client.fetches == 2 and doc.attempts == 2
    assert db.scalar(select(func.count()).select_from(DirectFeedRevision)) == 1
    no_public_writes(db)


def test_rejected_refresh_preserves_successful_original_revision(db):
    client = Source(submission())
    assert collect(db, client)['status'] == 'collected'
    doc = db.scalar(select(DirectFeedDocument))
    previous = db.scalar(select(DirectFeedRevision))
    preserved = (previous.id, previous.content_hash, previous.source_bytes, previous.parsed_json)
    doc.checked_at = datetime.now(timezone.utc) - timedelta(days=8)
    db.commit()
    client.raw = submission().replace(b'<tableValueTotal>300', b'<tableValueTotal>301')
    assert collect(db, client)['status'] == 'partial'
    assert doc.status == 'quarantined' and doc.content_hash != preserved[1]
    db.refresh(previous)
    assert (previous.id, previous.content_hash, previous.source_bytes, previous.parsed_json) == preserved
    assert db.scalar(select(func.count()).select_from(DirectFeedRevision)) == 2
    no_public_writes(db)


def test_failed_evidence_save_rolls_back_and_remains_retryable(db, monkeypatch):
    original = collector.record_document
    def fail_after_flush(*args, **kwargs):
        original(*args, **kwargs)
        raise RuntimeError('fixture storage failure')
    monkeypatch.setattr(collector, 'record_document', fail_after_flush)
    client = Source(submission().replace(b'<tableValueTotal>300', b'<tableValueTotal>301'))
    assert collect(db, client)['status'] == 'partial'
    doc = db.scalar(select(DirectFeedDocument))
    assert doc.status == 'failed' and 'storage failure' in doc.error
    assert doc.content_hash is None and doc.attempts == 1
    assert db.scalar(select(func.count()).select_from(DirectFeedRevision)) == 0
    monkeypatch.setattr(collector, 'record_document', original)
    doc.checked_at = datetime.now(timezone.utc) - timedelta(hours=2)
    db.commit()
    assert collect(db, client)['results'] == {'quarantined': 1}
    no_public_writes(db)


def test_transport_failure_does_not_become_source_quarantine(db):
    client = Source(DirectSourceError('Source transport failed: fixture'))
    assert collect(db, client)['status'] == 'partial'
    doc = db.scalar(select(DirectFeedDocument))
    assert doc.status == 'failed'
    assert db.scalar(select(func.count()).select_from(DirectFeedRevision)) == 0
    doc.checked_at = datetime.now(timezone.utc) - timedelta(hours=2)
    db.commit()
    collect(db, client)
    assert client.fetches == 2
    no_public_writes(db)
