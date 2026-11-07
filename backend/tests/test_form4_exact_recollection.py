from datetime import date, timedelta
import json

import pytest
from sqlalchemy import select

from app.models import Event, InsiderTransactionNormalized
from app.services import direct_feed_collection as collection
from app.services import direct_feed_worker as worker
from app.services.direct_feed_store import DirectFeedDocument, DirectFeedRevision, dumps, reconcile_insider
from app.services.feed_source_control import FeedSourceControl
from app.services.sec_form4_recollection import reconcile_published_original
from test_direct_feed_worker import db, stage, activate, counts


def prepared(db):
    ident, original = stage(db)
    activate(db)
    assert worker.publish_document(db, ident)['status'] == 'published'
    _, parsed, reasons = collection.parse_document('sec_form4', original['raw'], original['metadata'])
    assert not reasons and reconcile_insider(db, parsed)['ambiguous']
    return db.get(DirectFeedDocument, ident), original, parsed


def canonical(db):
    return dumps({model.__tablename__: [
        {c.name: getattr(row, c.name) for c in model.__table__.columns}
        for row in db.scalars(select(model).order_by(model.id))]
        for model in (Event, InsiderTransactionNormalized, worker.DirectFeedPublication)})


def test_exact_published_lots_recheck_without_canonical_changes(db):
    document, original, parsed = prepared(db)
    before = canonical(db)
    first = reconcile_published_original(db, document, original['raw'], parsed)
    assert first is not None
    assert len(first['matched']) == 4
    assert len({row['existing_id'] for row in first['matched']}) == 4
    assert first['ambiguous'] == first['existing_only_ids'] == first['unmatched'] == []
    assert reconcile_published_original(db, document, original['raw'], parsed) == first
    assert canonical(db) == before


def test_ordinary_collection_preserves_published_source_and_repeat(db, monkeypatch):
    document, original, parsed = prepared(db)
    before = canonical(db)
    count_before = counts(db)
    monkeypatch.setattr(collection, 'discover_sources', lambda *a, **k: (0, []))
    class Client:
        calls = 0
        def get(self, url):
            assert url == original['metadata']['url']
            self.calls += 1
            return original['raw']
    client = Client()
    for _ in range(2):
        document.checked_at = None
        db.commit()
        result = collection.collect_direct_feeds(db, client, sources=['sec_form4'],
            start=date(2026, 6, 1), end=date(2026, 6, 3), limit=1)
        assert result['processed'] == 1 and result['public_writes'] == 0
        db.refresh(document)
        assert document.status == 'parsed' and document.error is None
        assert len(json.loads(document.reconciliation_json)['matched']) == 4
        assert worker.publish_document(db, document.id)['status'] == 'existing'
        assert canonical(db) == before and counts(db) == count_before
    assert client.calls == 2


@pytest.mark.parametrize('fault', [
    'no_receipt', 'legacy_receipt', 'receipt_source', 'metadata', 'generation',
    'boundary', 'provider', 'raw', 'canonical_price', 'event_date', 'extra_row',
    'missing_row', 'source_price', 'source_hash', 'source_lot', 'duplicate_flag',
    'receipt_hashes', 'receipt_events', 'canonical_receipt', 'repeated_receipt_hash',
])
def test_unverified_or_changed_lots_retain_hold(db, fault):
    document, original, parsed = prepared(db)
    receipt = db.scalar(select(worker.DirectFeedPublication))
    control = db.get(FeedSourceControl, 'sec_form4')
    row = db.scalar(select(InsiderTransactionNormalized))
    raw = original['raw']
    if fault == 'no_receipt': db.delete(receipt)
    elif fault == 'legacy_receipt': receipt.status = 'existing'
    elif fault == 'receipt_source': receipt.source_hash = 'f' * 64
    elif fault == 'metadata': document.metadata_json = dumps({**original['metadata'], 'extra': True})
    elif fault == 'generation': control.generation += 1
    elif fault == 'boundary': control.publish_since -= timedelta(days=1)
    elif fault == 'provider': control.provider = 'fmp'
    elif fault == 'raw': raw += b'\n'
    elif fault == 'canonical_price': row.price += 1
    elif fault == 'event_date': db.scalar(select(Event)).event_date -= timedelta(days=1)
    elif fault == 'extra_row':
        copied = {c.name: getattr(row, c.name) for c in row.__table__.columns if c.name not in {'id', 'created_at', 'updated_at'}}
        db.add(InsiderTransactionNormalized(**{**copied, 'normalized_hash': 'extra'}))
    elif fault == 'missing_row': db.delete(row)
    elif fault == 'source_price': parsed['transactions'][0]['price'] += 1
    elif fault == 'source_hash': parsed['transactions'][0]['normalized_hash'] = 'wrong'
    elif fault == 'source_lot': parsed['transactions'].append(dict(parsed['transactions'][0]))
    elif fault == 'duplicate_flag': row.is_duplicate = True
    else:
        report = json.loads(receipt.report_json)
        if fault == 'receipt_hashes': report['normalized_hashes'] = []
        elif fault == 'repeated_receipt_hash': report['normalized_hashes'].append(report['normalized_hashes'][0])
        elif fault == 'receipt_events': report['event_ids'].append(report['event_ids'][0])
        elif fault == 'canonical_receipt': report['canonical_sha256'] = None
        receipt.report_json = dumps(report)
    db.commit()
    before = canonical(db)
    assert reconcile_published_original(db, document, raw, parsed) is None
    assert canonical(db) == before
