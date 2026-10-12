"""Unreadable House originals stay auditable, held and deduplicated."""
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
import hashlib
import json

import pytest
from sqlalchemy import func, select
from app.clients.direct_sources import DirectSourceError
from app.clients import house_ptr_review
from app.models import Event, EmailDelivery
from app.services import direct_feed_collection as collector
from app.services.direct_feed_store import DirectFeedDocument, DirectFeedRevision, discover
from test_direct_feeds import db


class Source:
    def __init__(self, raw):
        self.raw, self.fetches = raw, 0

    def get(self, url):
        self.fetches += 1
        if isinstance(self.raw, Exception):
            raise self.raw
        return self.raw


@pytest.fixture
def setup_scan(db, monkeypatch):
    raw = (Path(__file__).with_name('fixtures') / 'house_9116361.pdf').read_bytes()
    meta = json.loads(house_ptr_review.REGISTRY.read_bytes())['reviews'][0]['metadata']
    doc = discover(db, 'house_ptr', meta)
    db.commit()
    monkeypatch.setattr(collector, 'discover_sources', lambda *a, **k: ({}, []))
    monkeypatch.setattr(house_ptr_review, 'reviewed_house_scan', lambda *a: None)
    def forbidden(*a, **k):
        raise AssertionError('Unreadable scans must not reach reconciliation')
    monkeypatch.setattr(collector, 'reconcile_congress', forbidden)
    return doc, Source(raw)


def collect(db, source):
    return collector.collect_direct_feeds(db, source, sources=['house_ptr'],
        start=date(2026, 10, 1), end=date(2026, 10, 12), recheck_hours=24)


def test_scan_original_retained_without_publication_or_hourly_retry(db, setup_scan):
    doc, source = setup_scan
    first = collect(db, source)
    assert first['status'] == 'partial' and not first['cutover_ready']
    assert first['results'] == {'quarantined': 1}
    assert doc.status == 'quarantined' and doc.attempts == 1
    revision = db.scalar(select(DirectFeedRevision))
    assert revision.source_bytes == source.raw
    assert revision.content_hash == doc.content_hash == hashlib.sha256(source.raw).hexdigest()
    assert revision.source_text == ''
    parsed = json.loads(doc.parsed_json)
    assert parsed['transactions'] == []
    assert parsed['validation']['reason_code'] == 'house_scan_review_required'
    assert parsed['validation']['page_count'] == 1
    assert doc.reconciliation_json is None
    doc.checked_at = datetime.now(timezone.utc) - timedelta(hours=2)
    db.commit()
    assert collect(db, source)['processed'] == 0
    assert source.fetches == 1
    doc.checked_at = datetime.now(timezone.utc) - timedelta(hours=25)
    db.commit()
    assert collect(db, source)['results'] == {'quarantined': 1}
    assert doc.attempts == source.fetches == 2
    assert db.scalar(select(func.count()).select_from(DirectFeedRevision)) == 1
    assert db.scalar(select(func.count()).select_from(Event)) == 0
    assert db.scalar(select(func.count()).select_from(EmailDelivery)) == 0


@pytest.mark.parametrize('failure', [DirectSourceError('Source transport failed: fixture'), b'not a PDF'])
def test_transport_and_invalid_pdf_remain_retryable_failures(db, setup_scan, failure):
    doc, source = setup_scan
    source.raw = failure
    assert collect(db, source)['status'] == 'partial'
    assert doc.status == 'failed'
    assert db.scalar(select(func.count()).select_from(DirectFeedRevision)) == 0
    doc.checked_at = datetime.now(timezone.utc) - timedelta(hours=2)
    db.commit()
    collect(db, source)
    assert source.fetches == 2


def test_evidence_save_failure_rolls_back_and_can_retry(db, setup_scan, monkeypatch):
    doc, source = setup_scan
    original = collector.record_document
    def fail(*a, **k):
        original(*a, **k)
        raise RuntimeError('fixture storage failure')
    monkeypatch.setattr(collector, 'record_document', fail)
    collect(db, source)
    assert doc.status == 'failed' and doc.attempts == 1
    assert 'fixture storage failure' in doc.error
    assert doc.content_hash is None
    assert db.scalar(select(func.count()).select_from(DirectFeedRevision)) == 0
    monkeypatch.setattr(collector, 'record_document', original)
    doc.checked_at = datetime.now(timezone.utc) - timedelta(hours=2)
    db.commit()
    assert collect(db, source)['results'] == {'quarantined': 1}


def test_changed_scan_retains_both_immutable_originals(db, setup_scan):
    doc, source = setup_scan
    collect(db, source)
    original = db.scalar(select(DirectFeedRevision))
    saved = (original.content_hash, original.source_bytes, original.parsed_json)
    source.raw += b'\n% changed source revision\n'
    doc.checked_at = datetime.now(timezone.utc) - timedelta(hours=25)
    db.commit()
    collect(db, source)
    db.refresh(original)
    assert (original.content_hash, original.source_bytes, original.parsed_json) == saved
    assert doc.content_hash != original.content_hash
    assert db.scalar(select(func.count()).select_from(DirectFeedRevision)) == 2
