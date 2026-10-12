import json
from pathlib import Path
import sys

import pytest
from sqlalchemy import select, func, text

from app.jobs.check_direct_13f_capacity import JOB_KEY, require_queue_ownership, prepare_readonly
from app.services import direct_13f_batch as batch
from app.services.direct_feed_worker import DirectFeedPublication
from app.services.feed_source_control import FeedSourceControl
from app.models import Event, EmailDelivery, InsightsSnapshot
from test_direct_13f_worker import db, stage, pair

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
import scheduled_jobs as jobs


def test_preparation_reads_fmp_owned_originals_without_writes_or_transaction_changes(db, monkeypatch):
    ids = [stage(db, d) for d in pair()]
    control = db.get(FeedSourceControl, 'sec_13f')
    control.provider = 'fmp'
    db.commit()
    db.execute(text('PRAGMA query_only=ON'))
    transaction = db.get_transaction()
    monkeypatch.setattr(db, 'commit', lambda: pytest.fail('commit during preparation'))
    monkeypatch.setattr(db, 'rollback', lambda: pytest.fail('rollback during preparation'))
    monkeypatch.setattr(batch, 'publish_13f_document', lambda *a, **k: pytest.fail('publisher invoked'))
    selected, retries, _ = batch.select_13f_batch(db, generation=control.generation, limit=20, include_waiting=True)
    assert selected == ids and retries == []
    phases = []
    prepared = batch.prepare_13f_batch(db, ids=ids, identifier_documents=[], checkpoint=phases.append,
                                     use_staged_references=True)
    assert prepared['summary']['comparison_documents'] == 2
    assert set(prepared['groups']) == {'2026-06-30', '2026-09-30'}
    assert phases[-1] == 'complete' and phases.count('complete_peer_scan') == 2
    assert db.get_transaction() is transaction
    assert db.get(FeedSourceControl, 'sec_13f').provider == 'fmp'
    for model in (DirectFeedPublication, Event, EmailDelivery):
        assert db.scalar(select(func.count()).select_from(model)) == 0
    assert db.get(InsightsSnapshot, batch.RETRY_POSITION_KEY) is None


def test_preparation_budget_interrupts_before_publication(db, monkeypatch):
    ids = [stage(db, d) for d in pair()]
    def stop(phase):
        if phase == 'complete_peer_scan':
            raise RuntimeError('budget')
    monkeypatch.setattr(batch, 'publish_13f_document', lambda *a, **k: pytest.fail('published'))
    with pytest.raises(RuntimeError, match='budget'):
        batch.prepare_13f_batch(db, ids=ids, identifier_documents=[], checkpoint=stop)
    assert db.scalar(select(func.count()).select_from(DirectFeedPublication)) == 0


def test_diagnostic_rejects_non_postgres_session(db):
    with pytest.raises(RuntimeError, match='read_only_postgresql_required'):
        prepare_readonly(db, checkpoint=lambda p: None, identifier_manifest='unused')


def test_manual_capacity_job_has_no_calendar_and_serializes_with_imports(tmp_path):
    definition = jobs.MANUAL_JOBS[JOB_KEY]
    assert definition['lane'] == 'data'
    source = (Path(__file__).resolve().parents[1] / 'crontab').read_text()
    _, calendar = jobs.compile_schedule(source, Path('worker'), Path('manifest'), Path('queue'))
    assert JOB_KEY not in calendar and definition not in calendar.values()
    manifest_path = tmp_path / 'manifest.json'
    manifest_path.write_text(json.dumps(calendar))
    manifest = jobs.load_manifest(manifest_path)
    assert manifest[JOB_KEY] == definition
    with jobs.database(tmp_path / 'queue') as queue:
        jobs.enqueue(queue, JOB_KEY, definition, now=1)
        running = jobs.claim(queue, 'data', manifest, now=2)
        jobs.enqueue(queue, 'external-ingest-core', manifest['external-ingest-core'], now=3)
        assert jobs.claim(queue, 'data', manifest, now=4) is None
        jobs.finish(queue, running, 0)
        assert jobs.claim(queue, 'data', manifest, now=5)['key'] == 'external-ingest-core'


@pytest.mark.parametrize('change', ['none', 'token', 'pid', 'key', 'second_owner', 'finished'])
def test_diagnostic_requires_exact_live_scheduler_claim(tmp_path, change):
    queue_path = tmp_path / 'queue'
    with jobs.database(queue_path) as queue:
        jobs.enqueue(queue, JOB_KEY, jobs.MANUAL_JOBS[JOB_KEY], now=1)
        running = jobs.claim(queue, 'data', jobs.MANUAL_JOBS, now=2)
        queue.execute('UPDATE jobs SET pid=123 WHERE key=?', (JOB_KEY,))
        token, pid = running['token'], 123
        if change == 'token': token = 'forged'
        if change == 'pid': pid = 456
        if change == 'key': queue.execute('UPDATE jobs SET key=?', ('another-job',))
        if change == 'second_owner':
            queue.execute("INSERT INTO jobs(key,lane,started_at,token,pid) VALUES('another-job','data',2,'second',456)")
        if change == 'finished': jobs.finish(queue, running, 0)
    if change == 'none':
        require_queue_ownership(queue_path, token, pid)
    else:
        with pytest.raises(RuntimeError, match='not_exclusive_data_lane_owner'):
            require_queue_ownership(queue_path, token, pid)
