from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from app.models import DataEnrichmentJob
from app.services import data_enrichment_queue as queue


@pytest.mark.parametrize('provider,kind,age,held', [
    ('massive', 'quote', 59, True),
    ('massive', 'quote', 60, False),
    ('massive', 'quote', 900, False),
    ('fmp', 'quote', 900, True),
    ('fmp', 'quote', 3600, False),
    ('massive', 'price_series', 900, True),
    ('massive', 'ticker_context_bundle', 900, True),
    ('massive', 'fundamentals', 900, True),
])
def test_cooldown_respects_quote_provider_and_other_jobs(monkeypatch, provider, kind, age, held):
    monkeypatch.setenv('STOCK_PRICE_PROVIDER', provider)
    now = datetime(2026, 11, 20, 21, 0, tzinfo=timezone.utc)
    job = DataEnrichmentJob(job_type=kind, updated_at=now-timedelta(seconds=age))
    assert queue._job_completed_recently(job, now) is held
    job.updated_at = job.updated_at.replace(tzinfo=None)
    assert queue._job_completed_recently(job, now) is held


def test_completed_quote_requeues_on_next_worker_cycle_without_duplicates(monkeypatch):
    monkeypatch.setenv('STOCK_PRICE_PROVIDER', 'massive')
    monkeypatch.setenv('FMP_BACKGROUND_REFRESH_ENABLED', 'true')
    engine = create_engine('sqlite:///:memory:')
    DataEnrichmentJob.__table__.create(engine)
    sessions = sessionmaker(bind=engine)
    monkeypatch.setattr(queue, 'SessionLocal', sessions)
    assert queue.enqueue_data_enrichment_job(job_type='quote', symbol='NBIS')
    with sessions() as db:
        job = db.execute(select(DataEnrichmentJob)).scalar_one()
        original_id = job.id
        job.status = 'done'
        job.updated_at = datetime.now(timezone.utc)-timedelta(minutes=15)
        db.commit()
    assert queue._enqueue_skip_reason(job_type='quote', symbol='NBIS') == 'skipped_enqueue_failed'
    assert queue.enqueue_data_enrichment_job(job_type='quote', symbol='NBIS')
    assert not queue.enqueue_data_enrichment_job(job_type='quote', symbol='NBIS')
    with sessions() as db:
        job = db.execute(select(DataEnrichmentJob)).scalar_one()
        assert job.id == original_id
        assert job.status == 'queued'
        job.status = 'running'
        db.commit()
    assert not queue.enqueue_data_enrichment_job(job_type='quote', symbol='NBIS')
    with sessions() as db:
        job = db.get(DataEnrichmentJob, original_id)
        job.status = 'done'
        job.updated_at = datetime.now(timezone.utc)
        db.commit()
    assert not queue.enqueue_data_enrichment_job(job_type='quote', symbol='NBIS')
    assert queue._enqueue_skip_reason(job_type='quote', symbol='NBIS') == 'skipped_fresh'


@pytest.mark.parametrize('provider', ['massive', 'fmp'])
def test_page_quote_demand_precedes_metadata_without_losing_overdue_slots(monkeypatch, provider):
    monkeypatch.setenv('STOCK_PRICE_PROVIDER', provider)
    engine = create_engine('sqlite:///:memory:')
    DataEnrichmentJob.__table__.create(engine)
    sessions = sessionmaker(bind=engine)
    now = datetime.now(timezone.utc)
    with sessions() as db:
        for i in range(200):
            db.add(DataEnrichmentJob(job_type='ticker_meta', symbol='AAPL',
                dedupe_key=f'metadata-{i}', status='queued', priority=5,
                next_run_at=now-timedelta(hours=2)))
        for i in range(100):
            db.add(DataEnrichmentJob(job_type='quote', symbol=f'STOCK{i}',
                dedupe_key=f'quote-{i}', reason='massive_quote_stale_or_missing',
                status='queued', priority=20, next_run_at=now-timedelta(minutes=2)))
        for kind in ['fundamentals','price_series','ticker_context_bundle']:
            db.add(DataEnrichmentJob(job_type=kind, symbol='AAPL',dedupe_key=kind,
                status='queued',priority=100,next_run_at=now-timedelta(hours=3)))
        db.add(DataEnrichmentJob(job_type='quote', symbol='FUTURE',dedupe_key='future',
            reason='massive_quote_stale_or_missing',status='queued',priority=1,
            next_run_at=now+timedelta(minutes=1)))
        db.commit()
        rows=queue._select_enrichment_jobs(db,now=now,limit=25)
        assert len(rows)==25 and len({r['id'] for r in rows})==25
        assert all(r['symbol']!='FUTURE' for r in rows)
        assert {'fundamentals','price_series','ticker_context_bundle'} <= {r['job_type'] for r in rows}
        if provider=='massive':
            assert sum(r['job_type']=='quote' for r in rows)==20
            assert all(r['job_type']=='quote' for r in rows[:4])
        else:
            assert all(r['job_type']!='quote' for r in rows)
        restricted=queue._select_enrichment_jobs(db,now=now,limit=25,job_type='ticker_meta',symbols=['AAPL'])
        assert len(restricted)==25 and all(r['job_type']=='ticker_meta' for r in restricted)
