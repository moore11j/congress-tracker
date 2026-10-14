from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from app.models import DataEnrichmentJob
from app.services import data_enrichment_queue as queue


def fixture(monkeypatch):
    engine = create_engine("sqlite:///:memory:")
    DataEnrichmentJob.__table__.create(engine)
    factory = sessionmaker(bind=engine)
    monkeypatch.setattr(queue, "SessionLocal", factory)
    monkeypatch.setenv("ENRICHMENT_QUEUE_ENABLED", "true")
    monkeypatch.setenv("FMP_BACKGROUND_REFRESH_ENABLED", "true")
    monkeypatch.setenv("DATA_ENRICHMENT_QUEUE_PER_JOB_GUARD_ENABLED", "false")
    return factory(), datetime.now(timezone.utc)


def add(db, now, kind, count, priority, age=90, **kwargs):
    rows = [DataEnrichmentJob(
        job_type=kind, symbol="AAPL", source="test", reason=kwargs.get("reason", "test"),
        dedupe_key=f"{kind}:{priority}:{age}:{index}:{kwargs.get('reason')}",
        priority=priority, status=kwargs.get("status", "queued"),
        next_run_at=now - timedelta(minutes=age),
        created_at=now - timedelta(days=100), updated_at=now,
    ) for index in range(count)]
    db.add_all(rows)
    db.commit()
    return [r.id for r in rows]


def test_ordinary_batch_advances_every_overdue_kind_under_quote_and_metadata_flood(monkeypatch):
    db, now = fixture(monkeypatch)
    add(db, now, "quote", 500, 5)
    add(db, now, "ticker_meta", 1300, 15)
    kinds = ["cik_meta", "fundamentals", "press_releases", "news_stock", "ticker_financials",
             "sec_filings", "technical_indicators", "ticker_context_bundle", "profile"]
    for kind in kinds:
        add(db, now, kind, 30, 60)
    future = add(db, now, "press_releases", 1, 1, age=-10)
    seen = []
    monkeypatch.setattr(queue, "_process_one", lambda _db, job: seen.append((job.id, job.job_type)))
    result = queue.process_data_enrichment_jobs(limit=120)
    assert result == {"processed": 120, "succeeded": 120, "failed": 0, "skipped": 0}
    assert len({i for i, _ in seen}) == 120
    assert sum(kind == "quote" for _, kind in seen) >= 96
    assert set(kinds) <= {kind for _, kind in seen}
    assert not set(future) & {i for i, _ in seen}
    assert [kind for _, kind in seen[:4]] == ["quote"] * 4
    assert seen[4][1] != "quote"
    db.expire_all()
    assert sum(row.status == "done" for row in db.scalars(select(DataEnrichmentJob))) == 120


def test_fair_share_is_interleaved_before_pressure_yield(monkeypatch):
    db, now = fixture(monkeypatch)
    add(db, now, "quote", 200, 5)
    add(db, now, "press_releases", 10, 50)
    seen = []
    monkeypatch.setattr(queue, "_process_one", lambda _db, job: seen.append(job.job_type))
    monkeypatch.setenv("DATA_ENRICHMENT_QUEUE_PER_JOB_GUARD_ENABLED", "true")
    monkeypatch.setattr(queue, "_check_enrichment_queue_pressure", lambda _db: SimpleNamespace(
        proceed=len(seen) < 5, reason="pressure", to_dict=lambda: {}))
    result = queue.process_data_enrichment_jobs(limit=120)
    assert seen == ["quote"] * 4 + ["press_releases"]
    assert result == {"processed": 5, "succeeded": 5, "failed": 0, "skipped": 115}


def test_recent_work_and_small_batches_keep_priority_order(monkeypatch):
    db, now = fixture(monkeypatch)
    quote = add(db, now, "quote", 6, 5, age=2)
    add(db, now, "press_releases", 6, 50, age=90)
    assert [r["id"] for r in queue._select_enrichment_jobs(db, now=now, limit=4)] == quote[:4]
    db.query(DataEnrichmentJob).filter(DataEnrichmentJob.job_type == "press_releases").update(
        {"next_run_at": now - timedelta(minutes=2)})
    db.commit()
    assert [r["id"] for r in queue._select_enrichment_jobs(db, now=now, limit=5)] == quote[:5]


def test_requeued_jobs_use_current_wait_not_original_creation(monkeypatch):
    db, now = fixture(monkeypatch)
    recent = add(db, now, "quote", 1, 5, age=1)
    older = add(db, now, "quote", 1, 5, age=60)
    db.get(DataEnrichmentJob, recent[0]).created_at = now - timedelta(days=300)
    db.commit()
    assert [r["id"] for r in queue._select_enrichment_jobs(db, now=now, limit=2)] == older + recent


def test_fair_selection_respects_filters_retries_and_terminal_states(monkeypatch):
    db, now = fixture(monkeypatch)
    wanted = add(db, now, "press_releases", 20, 50, reason="wanted")
    add(db, now, "quote", 20, 5)
    add(db, now, "press_releases", 20, 1, reason="other")
    add(db, now, "press_releases", 1, 1, age=-1, reason="wanted")
    add(db, now, "press_releases", 1, 1, age=91, reason="wanted", status="done")
    selected = queue._select_enrichment_jobs(db, now=now, limit=10, job_type="press_releases",
                                            reason="wanted", symbols=["aapl"])
    assert len(selected) == 10
    assert {r["id"] for r in selected} <= set(wanted)
    assert len({r["id"] for r in selected}) == 10
    assert queue._select_enrichment_jobs(db, now=now, limit=10, symbols=["MSFT"]) == []


def test_overdue_failures_keep_retry_backoff_and_other_work_progresses(monkeypatch):
    db, now = fixture(monkeypatch)
    add(db, now, "quote", 30, 5)
    press = add(db, now, "press_releases", 1, 50)
    def process(_db, job):
        if job.job_type == "press_releases":
            raise queue.RetryableProviderTimeout()
    monkeypatch.setattr(queue, "_process_one", process)
    result = queue.process_data_enrichment_jobs(limit=10)
    assert result == {"processed": 10, "succeeded": 9, "failed": 1, "skipped": 0}
    db.expire_all()
    row = db.get(DataEnrichmentJob, press[0])
    assert row.status == "queued" and row.attempts == 1
    assert row.next_run_at.replace(tzinfo=timezone.utc) > now
    assert press[0] not in {r["id"] for r in queue._select_enrichment_jobs(db, now=now, limit=10)}


def test_deadline_leaves_unprocessed_jobs_queued(monkeypatch):
    db, now = fixture(monkeypatch)
    add(db, now, "quote", 30, 5)
    add(db, now, "press_releases", 10, 50)
    seen = []
    clock = iter(range(100))
    monkeypatch.setattr(queue.time, "monotonic", lambda: next(clock))
    monkeypatch.setattr(queue, "_process_one", lambda _db, job: seen.append(job.job_type))
    result = queue.process_data_enrichment_jobs(limit=10, max_seconds=6)
    assert seen == ["quote"] * 4 + ["press_releases"]
    assert result == {"processed": 5, "succeeded": 5, "failed": 0, "skipped": 5}
    db.expire_all()
    assert sum(row.status == "queued" for row in db.scalars(select(DataEnrichmentJob))) == 35
