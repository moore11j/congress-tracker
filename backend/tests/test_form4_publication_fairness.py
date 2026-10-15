from datetime import datetime, timedelta, timezone

from sqlalchemy import select
from app.services import direct_feed_worker as worker
from app.services.direct_feed_store import DirectFeedDocument, discover
from test_direct_feed_worker import db, activate, stage, counts


def held_documents(db, count, *, changed=False):
    now = datetime.now(timezone.utc)
    identifiers = []
    for index in range(count):
        key = f'0000320193-26-{index+100:06d}'
        metadata = dict(key=key, cik='320193', filing_date='2026-06-03',
            url=f'https://www.sec.gov/Archives/edgar/data/320193/{key}.txt')
        row = discover(db, 'sec_form4', metadata)
        row.status, row.content_hash = 'parsed', 'missing-original'
        row.first_seen_at = now-timedelta(days=3)
        db.commit()
        identifiers.append(row.id)
        assert worker.publish_document(db, row.id)['status'] == 'held'
        receipt = db.scalar(select(worker.DirectFeedPublication).where(
            worker.DirectFeedPublication.document_id == row.id))
        receipt.updated_at = now-timedelta(hours=1)
        if changed:
            row.content_hash = 'changed-after-hold'
        db.commit()
    return identifiers


def test_two_hundred_holds_cannot_block_new_valid_source_and_repeat(db):
    activate(db)
    held = held_documents(db, 200)
    valid, _ = stage(db)
    db.get(DirectFeedDocument, valid).first_seen_at = datetime.now(timezone.utc)-timedelta(days=1)
    db.commit()
    first = worker.publish_batch(db, limit=200, retry_held=True)
    assert first['processed'] == 200
    assert first['inserted_events'] == 4
    assert valid in [r['document_id'] for r in first['results']]
    before = counts(db)
    second = worker.publish_batch(db, limit=200, retry_held=True)
    assert second['processed'] == 200
    assert second['inserted_events'] == 0
    assert counts(db) == before
    assert set(r['document_id'] for r in second['results']) == set(held)
    assert first['email_deliveries'] == second['email_deliveries'] == 0


def test_retry_rotation_visits_every_hold_with_single_slot(db):
    activate(db)
    held = held_documents(db, 7)
    visits = [worker.publish_batch(db, limit=1, retry_held=True)['results'][0]['document_id']
              for _ in range(7)]
    assert visits == held
    assert counts(db)[:4] == [0, 0, 0, 0]


def test_changed_source_hold_rotates_without_replacing_evidence(db):
    activate(db)
    held = held_documents(db, 2, changed=True)
    receipts = list(db.scalars(select(worker.DirectFeedPublication).order_by(worker.DirectFeedPublication.id)))
    before = [(r.source_hash,r.metadata_hash,r.report_json,r.source_generation,r.publish_since,r.status)
              for r in receipts]
    old_attempts = [r.updated_at for r in receipts]
    visits = [worker.publish_batch(db, limit=1, retry_held=True)['results'][0]
              for _ in range(2)]
    assert [r['document_id'] for r in visits] == held
    assert all(r['status'] == 'held' and 'Source changed' in r['reason'] for r in visits)
    db.expire_all()
    assert [(r.source_hash,r.metadata_hash,r.report_json,r.source_generation,r.publish_since,r.status)
            for r in receipts] == before
    assert all(r.updated_at > old for r,old in zip(receipts,old_attempts))
    assert counts(db)[:4] == [0, 0, 0, 0]


def test_retry_disabled_still_skips_held_documents(db):
    activate(db)
    held_documents(db, 2)
    valid, _ = stage(db)
    result = worker.publish_batch(db, limit=1, retry_held=False)
    assert result['results'][0]['document_id'] == valid
    assert result['inserted_events'] == 4
    assert worker.publish_batch(db, limit=1)['processed'] == 0


def test_recovered_hold_publishes_with_production_autoflush_disabled(db):
    from test_direct_feed_worker import test_held_original_bytes_recollection_keeps_one_revision_and_can_retry
    db.autoflush = False
    test_held_original_bytes_recollection_keeps_one_revision_and_can_retry(db)
