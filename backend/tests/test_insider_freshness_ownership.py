from datetime import date
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from app.db import Base
from app import ingest_run
from app.services.feed_source_control import FeedSourceControl, select_feed_source, FeedWriterBusy


@pytest.fixture
def engine(monkeypatch):
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    monkeypatch.setattr(ingest_run, 'SessionLocal', sessionmaker(bind=engine))
    monkeypatch.setattr(ingest_run, 'ensure_fmp_live_allowed', lambda **kwargs: None)
    monkeypatch.setattr(ingest_run, 'record_provider_response', lambda **kwargs: None)
    monkeypatch.setenv('FMP_API_KEY', 'fixture-only')
    yield engine
    engine.dispose()


@pytest.mark.parametrize('provider', ['paused', 'sec_edgar'])
def test_selected_source_retires_freshness_http_probe(engine, monkeypatch, provider):
    with Session(engine) as db:
        select_feed_source(db, feed='sec_form4', provider=provider, publish_since=date(2026,10,7),
            expected_generation=0, reason='test')
        db.commit()
    monkeypatch.setattr(ingest_run.requests, 'get', lambda *a, **k: pytest.fail('Legacy freshness HTTP escaped source selection'))
    assert ingest_run._check_insider_freshness() is None


def test_default_fmp_keeps_existing_diagnostic(engine, monkeypatch):
    calls = []
    class Response:
        status_code = 200
        def json(self): return [{'filingDate':'2026-10-06'}, {'filingDate':'2026-10-07'}]
    def get(*args, **kwargs):
        calls.append(True)
        return Response()
    monkeypatch.setattr(ingest_run.requests, 'get', get)
    assert ingest_run._check_insider_freshness() == '2026-10-07'
    assert calls == [True]


@pytest.mark.parametrize('failure', ['missing_schema', 'busy'])
def test_unverified_ownership_fails_closed(engine, monkeypatch, failure):
    if failure == 'missing_schema':
        FeedSourceControl.__table__.drop(engine)
    else:
        def busy(*args, **kwargs): raise FeedWriterBusy('test contention')
        monkeypatch.setattr('app.services.feed_source_control.require_selected_source', busy)
    monkeypatch.setattr(ingest_run.requests, 'get', lambda *a, **k: pytest.fail('Failed ownership opened HTTP'))
    assert ingest_run._check_insider_freshness() is None
