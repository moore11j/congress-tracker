import json

import pytest
from sqlalchemy import select, func

from app.models import Event, InsiderTransactionNormalized, DataEnrichmentJob
from app.services.direct_feed_rehearsal import snapshot
from app.services.direct_feed_store import discover, record_document, DirectFeedRevision
from app.services.direct_sec_repair import (
    SecRepairReceipt, inspect_staged_repair, rehearse_staged_repair,
    apply_reviewed_staged_repair,
)
from test_direct_feed_rehearsal import db, linked_events


def prepared(db):
    source, rows, raw, events = linked_events(db)
    meta = source['metadata']
    meta['url'] = f'https://www.sec.gov/Archives/edgar/data/{meta["cik"].lstrip("0")}/{meta["key"]}.txt'
    staged = discover(db, 'sec_form4', meta)
    record_document(db, staged, source['raw'], source['raw'].decode(), {}, reasons=['canonical conflict'])
    db.commit()
    return staged, rows, raw, events


def test_atomic_complete_repair_preserves_identity_dates_raw_and_repeat(db):
    staged, rows, raw, events = prepared(db)
    originals = [snapshot(row) for row in raw]
    identities = [(e.id, e.ts, e.event_date) for e in events]
    review = inspect_staged_repair(db, staged.id)
    assert review['status'] == 'planned'
    assert review['updates'] == {'events': 3, 'insider_transactions_normalized': 3}
    result = rehearse_staged_repair(db, staged.id, expected_plan_hash=review['plan_sha256'])
    assert result['updated'] == 6 and result['emails'] == 0
    db.commit()
    assert staged.status == 'parsed'
    assert [snapshot(row) for row in raw] == originals
    assert [(e.id, e.ts, e.event_date) for e in events] == identities
    assert db.scalar(select(func.count()).select_from(Event)) == 3
    assert db.scalar(select(func.count()).select_from(SecRepairReceipt)) == 1
    assert rehearse_staged_repair(db, staged.id, expected_plan_hash=review['plan_sha256'])['updated'] == 0
    receipt = db.get(SecRepairReceipt, staged.id)
    assert all(op['before'] != op['after'] for op in json.loads(receipt.plan_json)['operations'])


@pytest.mark.parametrize('change', ['raw', 'event', 'normalized', 'source', 'new_event'])
def test_review_drift_prevents_any_changes(db, change):
    staged, rows, raw, events = prepared(db)
    review = inspect_staged_repair(db, staged.id)
    if change == 'raw':
        raw[0].price += 1
    elif change == 'event':
        events[0].amount_min = 234
    elif change == 'normalized':
        rows[0].price += 1
    elif change == 'source':
        db.scalar(select(DirectFeedRevision)).source_bytes += b'changed'
    else:
        db.add(Event(event_type='insider_trade', source='fmp', symbol=events[0].symbol,
            ts=events[0].ts, event_date=events[0].event_date, payload_json=events[0].payload_json))
    db.commit()
    before = [snapshot(row) for row in rows]
    with pytest.raises(ValueError):
        rehearse_staged_repair(db, staged.id, expected_plan_hash=review['plan_sha256'])
    assert [snapshot(row) for row in rows] == before
    assert db.scalar(select(func.count()).select_from(SecRepairReceipt)) == 0


def test_ambiguous_event_holds_whole_filing(db):
    staged, rows, raw, events = prepared(db)
    db.delete(events[0])
    db.commit()
    review = inspect_staged_repair(db, staged.id)
    assert review['status'] == 'held'
    with pytest.raises(ValueError, match='complete, unambiguous'):
        rehearse_staged_repair(db, staged.id, expected_plan_hash=review['plan_sha256'])
    assert all(row.price == 999 for row in rows)


def test_running_enrichment_holds_and_receipt_tamper_rejected(db):
    staged, rows, raw, events = prepared(db)
    job = DataEnrichmentJob(job_type='pnl_refresh', status='running',
        window_key=f'event:{events[0].id}', dedupe_key='test-job', payload_json='{}')
    db.add(job)
    db.commit()
    assert inspect_staged_repair(db, staged.id)['status'] == 'held'
    job.status = 'done'
    db.commit()
    review = inspect_staged_repair(db, staged.id)
    rehearse_staged_repair(db, staged.id, expected_plan_hash=review['plan_sha256'])
    db.commit()
    receipt = db.get(SecRepairReceipt, staged.id)
    altered = json.loads(receipt.plan_json)
    altered['operations'][0]['before']['price'] = 1
    receipt.plan_json = json.dumps(altered)
    db.commit()
    with pytest.raises(ValueError, match='archived plan'):
        inspect_staged_repair(db, staged.id)


def test_production_entry_rejects_sqlite(db):
    with pytest.raises(ValueError, match='fresh PostgreSQL'):
        apply_reviewed_staged_repair(db, 1, expected_plan_hash='x', expected_generation=1)


def test_failure_after_updates_rolls_back_every_row_and_receipt(db, monkeypatch):
    import app.services.direct_sec_repair as repair
    staged, rows, raw, events = prepared(db)
    review = inspect_staged_repair(db, staged.id)
    before = [snapshot(row) for row in rows + events]
    monkeypatch.setattr(repair, 'reconcile_insider', lambda *_: dict(unmatched=[1], ambiguous=[], existing_only_ids=[]))
    with pytest.raises(ValueError, match='complete source reconciliation'):
        rehearse_staged_repair(db, staged.id, expected_plan_hash=review['plan_sha256'])
    assert [snapshot(row) for row in rows + events] == before
    assert db.scalar(select(func.count()).select_from(SecRepairReceipt)) == 0


def test_existing_alerts_repair_copies_without_resetting_read_state_or_delivery(db):
    from datetime import datetime
    from app.models import MonitoringAlert, EmailDelivery
    from app.services.monitoring_alerts import _ensure_alert_for_event
    from test_email_digests import _user, _watchlist
    staged, rows, raw, events = prepared(db)
    user = _user(db, 'private-sec-repair@example.test')
    watchlist = _watchlist(db, user)
    for event in events:
        assert _ensure_alert_for_event(db, user_id=user.id, watchlist=watchlist, event=event)
    db.commit()
    alerts = list(db.scalars(select(MonitoringAlert).order_by(MonitoringAlert.id)))
    alerts[0].read_at = datetime(2026, 6, 4)
    db.commit()
    before = [snapshot(alert) for alert in alerts]
    review = inspect_staged_repair(db, staged.id)
    assert 'private-sec-repair' not in json.dumps(review)
    assert review['updates']['monitoring_alerts'] == 3
    rehearse_staged_repair(db, staged.id, expected_plan_hash=review['plan_sha256'])
    db.commit()
    for old, alert in zip(before, alerts):
        current = snapshot(alert)
        assert {k: v for k, v in current.items() if k not in {'payload_json', 'title', 'body'}} == {
            k: v for k, v in old.items() if k not in {'payload_json', 'title', 'body'}}
        assert 'filed 2026-06-03' in alert.body
    assert db.scalar(select(func.count()).select_from(EmailDelivery)) == 0
    assert rehearse_staged_repair(db, staged.id, expected_plan_hash=review['plan_sha256'])['updated'] == 0
