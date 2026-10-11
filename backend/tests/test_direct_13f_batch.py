import hashlib
import json

import pytest
from sqlalchemy import func, select

from app.models import Event
from app.services import direct_13f_batch as batch
from app.services.direct_feed_store import DirectFeedDocument, DirectFeedRevision
from app.services.direct_feed_worker import DirectFeedPublication
from test_direct_13f_worker import db, stage, pair


def test_captured_batch_publishes_and_empty_repeat_does_not_fetch(db):
    prior, current = pair()
    stage(db, prior)
    stage(db, current)
    result = batch.publish_13f_batch(db, identifier_documents=[], limit=2)
    assert result['processed'] == 2 and result['feed_events'] > 0
    assert batch.publish_13f_batch(db, identifier_documents=[])['processed'] == 0
    assert db.scalar(select(func.count()).select_from(DirectFeedPublication)) == 2


def test_out_of_order_batch_requires_explicit_waiting_retry(db):
    prior, current = pair()
    stage(db, current)
    first = batch.publish_13f_batch(db, identifier_documents=[])
    assert first['status'] == 'partial' and first['results'][0]['status'] == 'waiting'
    stage(db, prior)
    assert batch.publish_13f_batch(db, identifier_documents=[])['processed'] == 1
    assert batch.publish_13f_batch(db, identifier_documents=[])['processed'] == 0
    retry = batch.publish_13f_batch(db, identifier_documents=[], retry_waiting=True)
    assert retry['processed'] == 1 and retry['feed_events'] > 0
    assert batch.publish_13f_batch(db, identifier_documents=[], retry_waiting=True)['processed'] == 0


def test_peer_transport_drift_aborts_before_any_publication(db):
    prior, current = pair()
    stage(db, prior)
    stage(db, current)
    revision = db.scalar(select(DirectFeedRevision))
    revision.source_bytes += b'changed'
    db.commit()
    with pytest.raises(ValueError, match='checksum'):
        batch.publish_13f_batch(db, identifier_documents=[])
    db.rollback()
    assert db.scalar(select(func.count()).select_from(DirectFeedPublication)) == 0
    assert db.scalar(select(func.count()).select_from(Event)) == 0


def test_held_receipt_cannot_starve_new_documents(db):
    prior, current = pair()
    doc_id = stage(db, prior)
    doc = db.get(DirectFeedDocument, doc_id)
    doc.metadata_json = json.dumps({**json.loads(doc.metadata_json), 'filing_date': '2099-01-01'})
    db.commit()
    stage(db, current)
    first = batch.publish_13f_batch(db, identifier_documents=[], limit=1)
    assert first['results'][0]['status'] == 'held'
    second = batch.publish_13f_batch(db, identifier_documents=[], limit=1)
    assert second['processed'] == 1 and second['results'][0]['document_id'] != doc_id


@pytest.mark.parametrize('tamper', ['checksum', 'escape', 'duplicate'])
def test_manifest_rejects_changed_or_unbounded_paths(tmp_path, tamper):
    folder = tmp_path / 'evidence'
    folder.mkdir()
    raw = b'reviewed'
    (folder / 'one.source').write_bytes(raw)
    entry = {'source_file': 'one.source', 'content_hash': hashlib.sha256(raw).hexdigest(), 'url': 'https://www.sec.gov/fixture'}
    entries = [entry]
    if tamper == 'checksum':
        (folder / 'one.source').write_bytes(b'changed')
    elif tamper == 'escape':
        (tmp_path / 'outside.source').write_bytes(raw)
        entry['source_file'] = '../outside.source'
    else:
        entries.append(dict(entry))
    manifest = folder / 'manifest.json'
    manifest.write_text(json.dumps({'documents': entries}))
    with pytest.raises(ValueError):
        batch.load_identifier_manifest(manifest)


def test_disabled_job_never_opens_manifest_or_database(monkeypatch, capsys):
    from app.jobs import publish_direct_13fs as job
    monkeypatch.delenv('DIRECT_13F_PUBLICATION_ENABLED', raising=False)
    monkeypatch.setattr('sys.argv', ['publisher'])
    monkeypatch.setattr(job, 'SessionLocal', lambda: pytest.fail('Opened DB'))
    monkeypatch.setattr(job, 'load_identifier_manifest', lambda *a: pytest.fail('Opened manifest'))
    job.main()
    assert json.loads(capsys.readouterr().out)['status'] == 'disabled'


def test_period_scoping_preserves_relevant_peers_within_unchanged_budget(db):
    prior, current = pair()
    stage(db, prior); stage(db, current)
    budget = len(current['raw'])
    with pytest.raises(ValueError, match='oversized'):
        batch.staged_comparisons(db, byte_budget=budget)
    peers = batch.staged_comparisons(db, byte_budget=budget, report_periods={'2026-09-30'})
    assert [row['content_hash'] for row in peers] == [current['content_hash']]
    # Exclusion is based on verified original bytes, never the saved parse.
    saved = db.scalar(select(DirectFeedRevision).where(DirectFeedRevision.content_hash == prior['content_hash']))
    saved.parsed_json = json.dumps({'metadata': {'report_period': '2026-09-30'}})
    db.commit()
    assert len(batch.staged_comparisons(db, byte_budget=budget, report_periods={'2026-09-30'})) == 1
    saved.source_bytes += b'changed'
    db.commit()
    with pytest.raises(ValueError, match='checksum'):
        batch.staged_comparisons(db, byte_budget=budget, report_periods={'2026-09-30'})


def test_relevant_period_cannot_bypass_evidence_budget(db):
    prior, current = pair()
    stage(db, current)
    with pytest.raises(ValueError, match='evidence budget'):
        batch.staged_comparisons(db, byte_budget=len(current['raw'])-1, report_periods={'2026-09-30'})


def _independent_peer(cik, serial, *, filed=None):
    from test_direct_13f_evidence import peer
    item = peer(cik, serial=serial, filed=filed)
    item['metadata']['url'] = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{item['metadata']['key']}.txt"
    return item


def test_new_other_quarter_sources_cannot_erase_existing_value_hold(db):
    from test_direct_13f_publication import document
    from app.services.direct_feed_collection import parse_document
    from app.services.direct_13f_evidence import value_consistency_issues
    target = document(rows=[('000361105', 100, 100, '')])
    target_id = stage(db, target)
    for cik, serial in [('0000000001', 2), ('0000000002', 3)]:
        stage(db, _independent_peer(cik, serial))
    _, parsed, _ = parse_document('sec_13f', target['raw'], target['metadata'])
    before = batch.staged_comparisons(db, byte_budget=100_000_000, report_periods={'2026-09-30'})
    expected = value_consistency_issues(parsed, before)
    assert expected
    for serial in range(4, 503):
        stage(db, document(quarter=2, serial=serial))
    stage(db, _independent_peer('0000000003', 503, filed='2026-10-07'))
    after = batch.staged_comparisons(db, byte_budget=100_000_000, report_periods={'2026-09-30'})
    assert len(after) == 4
    assert value_consistency_issues(parsed, after) == expected
    result = batch.publish_13f_batch(db, identifier_documents=[], limit=1)
    assert result['results'][0]['document_id'] == target_id
    assert result['results'][0]['status'] == 'held'
    assert result['results'][0]['value_issues'] == expected
    assert db.scalar(select(func.count()).select_from(Event)) == 0


def test_mixed_quarter_batch_keeps_complete_peers_and_target_order(db):
    from test_direct_13f_publication import document
    prior, current = pair()
    ids = [stage(db, prior), stage(db, current)]
    for serial in range(3, 503):
        stage(db, document(quarter=2 if serial % 2 else 3, serial=serial))
    result = batch.publish_13f_batch(db, identifier_documents=[], limit=2)
    assert [r['document_id'] for r in result['results']] == ids
    assert result['comparison_documents'] == 502
    assert result['comparison_group_counts'] == {'2026-06-30':251, '2026-09-30':251}
    assert result['feed_events'] > 0
    assert all(len(json.loads(r.report_json)['evidence']['comparisons']) == 251
               for r in db.scalars(select(DirectFeedPublication)))


@pytest.mark.parametrize('bound', ['inventory', 'period', 'bytes', 'missing'])
def test_incomplete_peer_inventory_stops_before_any_publication(db, monkeypatch, bound):
    from test_direct_13f_publication import document
    for serial in range(1, 4):
        stage(db, document(serial=serial))
    if bound == 'inventory': monkeypatch.setattr(batch, 'MAX_SCAN_DOCUMENTS', 2)
    if bound == 'period': monkeypatch.setattr(batch, 'MAX_PERIOD_DOCUMENTS', 2)
    if bound == 'bytes': monkeypatch.setattr(batch, 'MAX_SCAN_BYTES', 1)
    if bound == 'missing':
        db.scalar(select(DirectFeedRevision)).source_bytes = None
        db.commit()
    monkeypatch.setattr(batch, 'publish_13f_document', lambda *a, **k: pytest.fail('Partial evidence published'))
    with pytest.raises(ValueError):
        batch.publish_13f_batch(db, identifier_documents=[], limit=1)
    db.rollback()
    assert db.scalar(select(func.count()).select_from(DirectFeedPublication)) == 0
    assert db.scalar(select(func.count()).select_from(Event)) == 0


def test_chunked_source_fetch_preserves_each_original(db, monkeypatch):
    from test_direct_13f_publication import document
    sources = [document(serial=n) for n in range(1, 8)]
    for item in sources: stage(db, item)
    monkeypatch.setattr(batch, 'FETCH_DOCUMENTS', 2)
    monkeypatch.setattr(batch, 'FETCH_BYTES', len(sources[0]['raw']) + 1)
    result = batch.staged_comparisons(db, byte_budget=100_000_000, report_periods={'2026-09-30'})
    assert [(d['raw'], d['content_hash']) for d in result] == [(d['raw'],d['content_hash']) for d in reversed(sources)]


def test_separate_quarter_budgets_keep_complete_inputs_and_global_scan_bound(db, monkeypatch):
    prior, current = pair()
    stage(db, prior); stage(db, current)
    # Both individual groups fit; their union is not one prepared evidence object.
    budget = max(len(prior['raw']),len(current['raw']))
    groups = batch.staged_comparison_groups(db, byte_budget=budget,
        report_periods={'2026-06-30','2026-09-30'})
    assert {p:[d['content_hash'] for d in g] for p,g in groups.items()} == {
        '2026-06-30':[prior['content_hash']], '2026-09-30':[current['content_hash']]}
    with pytest.raises(ValueError, match='Combined comparison cohort exceeds evidence budget'):
        batch.staged_comparisons(db, byte_budget=budget, report_periods=set(groups))
    monkeypatch.setattr(batch, 'MAX_SCAN_BYTES', budget)
    with pytest.raises(ValueError, match='Complete staged source scan exceeds byte bounds'):
        batch.staged_comparison_groups(db, byte_budget=budget, report_periods=set(groups))
