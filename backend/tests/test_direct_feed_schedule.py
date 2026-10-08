from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
import json

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from app.db import Base
from app.clients.direct_sources import DirectSourceError
from app.services import direct_feed_collection as collection
from app.services.direct_feed_store import DirectFeedDocument, dumps
from test_direct_sec_collection import META, submission


@pytest.mark.parametrize('sources,expected', [
    (['house_ptr', 'senate_ptr'], date(2026, 10, 8)),
    (['senate_ptr'], date(2026, 10, 8)),
    (['house_ptr'], date(2026, 10, 8)),
    (['sec_form4', 'sec_13f'], date(2026, 10, 7)),
    (['senate_ptr', 'sec_form4'], date(2026, 10, 7)),
])
def test_discovery_boundary_includes_live_congress_without_unfinished_sec_index(sources, expected):
    from app.jobs.collect_direct_feeds import _default_end_date
    assert _default_end_date(sources, date(2026, 10, 8)) == expected


@pytest.fixture
def db():
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        yield session
    engine.dispose()


def add_document(db, key, *, status, checked_at=None):
    row = DirectFeedDocument(feed='sec_13f', source_key=key, source_url=f'https://www.sec.gov/{key}',
                             metadata_json=dumps(META), status=status, checked_at=checked_at)
    db.add(row)
    db.commit()
    return row


def collect(db, monkeypatch, client, **kwargs):
    monkeypatch.setattr(collection, 'discover_sources', lambda *a, **k: ({}, []))
    return collection.collect_direct_feeds(db, client, sources=['sec_13f'], start=date(2026,10,6),
                                           end=date(2026,10,6), recheck_hours=168, **kwargs)


def test_pending_backlog_precedes_stale_rechecks(db, monkeypatch):
    old = add_document(db, 'old', status='parsed', checked_at=datetime(2020,1,1))
    new = add_document(db, 'new', status='pending')
    class Client:
        def get(self, url):
            assert url == new.source_url
            return submission()
    result = collect(db, monkeypatch, Client(), limit=1)
    assert result['processed'] == 1 and old.attempts == 0 and new.attempts == 1


def test_failed_document_retries_after_one_hour_not_week(db, monkeypatch):
    now = datetime(2026,10,7,1,47,tzinfo=timezone.utc)
    monkeypatch.setattr(collection, 'utcnow', lambda: now)
    row = add_document(db, 'failure', status='failed', checked_at=now - timedelta(minutes=59))
    class Client:
        def get(self, url):
            return submission()
    assert collect(db, monkeypatch, Client())['processed'] == 0
    row.checked_at = now - timedelta(hours=1)
    db.commit()
    assert collect(db, monkeypatch, Client())['processed'] == 1


@pytest.mark.parametrize('message', ['Source HTTP 403: denied', 'Source HTTP 429: slow down',
                                   'Source transport failed: offline', 'Source cooldown active'])
def test_source_failure_stops_feed_without_discarding_backlog(db, monkeypatch, message):
    add_document(db, 'first', status='pending')
    add_document(db, 'second', status='pending')
    calls = []
    class Client:
        def get(self, url):
            calls.append(url)
            raise DirectSourceError(message)
    result = collect(db, monkeypatch, Client())
    assert len(calls) == len(result['errors']) == 1
    assert result['pending_documents'] == 1
    assert result['status'] == 'partial'
    assert sum(row.attempts for row in db.scalars(select(DirectFeedDocument))) == 1


@pytest.mark.parametrize('mode', ['off', 'paused', 'busy'])
def test_job_guards_do_not_fetch_or_initialize_schema(monkeypatch, capsys, mode):
    from app.jobs import collect_direct_feeds as job
    monkeypatch.setattr('sys.argv', ['collect_direct_feeds', '--sources', 'sec_form4', 'sec_13f'])
    monkeypatch.setenv('DIRECT_FEEDS_MODE', 'off' if mode == 'off' else 'shadow')
    monkeypatch.setenv('BACKGROUND_JOBS_PAUSED', 'true' if mode == 'paused' else 'false')
    def forbidden(*a, **k):
        pytest.fail('Guard allowed schema/network work')
    @contextmanager
    def busy():
        yield False
    monkeypatch.setattr(job, 'collector_lock', busy)
    monkeypatch.setattr(job, 'ensure_direct_feed_schema', forbidden)
    monkeypatch.setattr(job, 'DirectSourceClient', forbidden)
    job.main()
    result = json.loads(capsys.readouterr().out)
    if mode == 'paused':
        assert result['reason'] == 'background_jobs_paused'
    else:
        assert result['status'] == ('disabled' if mode == 'off' else 'busy')
