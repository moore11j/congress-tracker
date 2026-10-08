from datetime import date
import json

import pytest
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session, sessionmaker

from app.db import Base
from app.models import Event, InstitutionalFiling, InstitutionalPosition, DataEnrichmentJob, EmailDelivery
from app.services import direct_13f_worker as worker
from app.services.direct_feed_store import discover, record_document
from app.services.direct_feed_collection import parse_document
from app.services.direct_feed_worker import DirectFeedPublication
from app.services.feed_source_control import FeedSourceMismatch, select_feed_source, writer_transaction
from app.services import institutional_activity as activity
from test_direct_13f_publication import document, mappings, SINCE


@pytest.fixture
def db(monkeypatch):
    def forbidden(*a, **k): pytest.fail('Network/email in publication')
    monkeypatch.setattr('requests.sessions.Session.request', forbidden)
    monkeypatch.setattr('httpx.Client.send', forbidden)
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    with Session(engine) as db:
        mappings(db)
        select_feed_source(db, feed='sec_13f', provider='sec_edgar', publish_since=SINCE,
                           expected_generation=0, reason='Isolated fixture')
        db.commit()
        yield db
    engine.dispose()


def stage(db, doc):
    row = discover(db, doc['feed'], doc['metadata'])
    source, parsed, reasons = parse_document(doc['feed'], doc['raw'], doc['metadata'])
    record_document(db, row, doc['raw'], source, parsed, reasons=reasons)
    db.commit()
    return row.id


def pair():
    return [document(quarter=2, serial=2, rows=[('000361105', 10_000_000, 100_000_000, ''), ('000361106', 10_000_000, 100_000_000, '')]),
            document(rows=[('000361105', 30_000_000, 300_000_000, ''), ('000361107', 20_000_000, 200_000_000, '')])]


def publish(db, doc):
    return worker.publish_13f_document(db, stage(db, doc), identifier_documents=[], comparison_documents=[doc])


def test_guarded_pair_receipt_queue_and_repeat(db):
    prior, current = pair()
    assert publish(db, prior)['derived_state'] == 'historical_no_alerts'
    result = publish(db, current)
    assert result['derived_state'] == 'published' and result['feed_events'] > 0
    assert len(result['event_ids']) == result['feed_events']
    first = db.scalar(select(func.count()).select_from(DataEnrichmentJob))
    repeat = publish(db, current)
    assert repeat['status'] == 'existing' and repeat['feed_events'] == 0
    assert db.scalar(select(func.count()).select_from(DataEnrichmentJob)) == first
    assert db.scalar(select(func.count()).select_from(DirectFeedPublication)) == 2
    assert db.scalar(select(func.count()).select_from(EmailDelivery)) == 0


def test_waiting_prior_arrival_can_publish_once(db):
    prior, current = pair()
    assert publish(db, current)['status'] == 'waiting'
    assert publish(db, prior)['derived_state'] == 'historical_no_alerts'
    assert publish(db, current)['status'] == 'published'
    assert publish(db, current)['status'] == 'existing'


def test_canonical_drift_holds_without_new_events(db):
    prior, current = pair()
    publish(db, prior)
    publish(db, current)
    row = db.scalar(select(InstitutionalPosition).where(InstitutionalPosition.report_quarter == 3))
    row.shares += 1
    db.commit()
    assert publish(db, current)['status'] == 'held'


@pytest.mark.parametrize('mutation', ['payload', 'delete'])
def test_public_holder_event_drift_is_not_an_existing_pass(db, mutation):
    prior, current = pair()
    publish(db, prior)
    result = publish(db, current)
    assert result['holder_event_ids']
    event = db.get(Event, result['holder_event_ids'][0])
    if mutation == 'delete':
        db.delete(event)
    else:
        event.payload_json = '{}'
    db.commit()
    repeat = publish(db, current)
    assert repeat['status'] == 'held' and 'canonical state changed' in repeat['reason']


def test_failure_after_projection_rolls_back_publication_and_jobs(db, monkeypatch):
    prior, current = pair()
    publish(db, prior)
    doc_id = stage(db, current)
    def fail(**k): raise RuntimeError('interrupted cache update')
    monkeypatch.setattr(worker, 'bump_feed_events_epoch', fail)
    with pytest.raises(RuntimeError):
        worker.publish_13f_document(db, doc_id, identifier_documents=[], comparison_documents=[current])
    assert db.scalar(select(func.count()).select_from(InstitutionalFiling)) == 1
    assert db.scalar(select(func.count()).select_from(Event)) == 0
    assert db.scalar(select(func.count()).select_from(DataEnrichmentJob)) == 0
    assert db.scalar(select(func.count()).select_from(DirectFeedPublication)) == 1


def test_legacy_helpers_and_fetch_cannot_bypass_selected_direct_owner(db, monkeypatch):
    from app import ingest_institutional_activity as legacy
    with pytest.raises(FeedSourceMismatch):
        activity.upsert_institutional_holder(db, None)
    db.rollback()
    monkeypatch.setattr(legacy, 'SessionLocal', sessionmaker(bind=db.get_bind()))
    monkeypatch.setattr(legacy, 'ensure_institutional_activity_schema', lambda *a: None)
    monkeypatch.setattr(legacy, 'fetch_latest_institutional_filings', lambda **k: pytest.fail('FMP called'))
    with pytest.raises(FeedSourceMismatch):
        legacy.ingest_latest_institutional_filings()


def test_scope_and_cached_ownership_do_not_survive_commit(db):
    prior, _ = pair()
    publish(db, prior)
    with pytest.raises(FeedSourceMismatch):
        activity.upsert_institutional_holder(db, None)
    db.rollback()
    select_feed_source(db, feed='sec_13f', provider='paused', publish_since=SINCE,
                       expected_generation=1, reason='Pause fixture')
    db.commit()
    with pytest.raises(FeedSourceMismatch):
        with writer_transaction(db, 'sec_13f', 'sec_edgar'):
            pytest.fail('Stale ownership cache')


@pytest.mark.parametrize('helper,args', [
    (activity.upsert_holder_performance_rows, ('1', [])),
    (activity.upsert_holder_industry_breakdown_rows, ('1', 2026, 3, [])),
    (activity.upsert_industry_summary_rows, (2026, 3, [])),
])
def test_provider_enrichment_cannot_overwrite_direct_holdings_context(db, helper, args):
    with pytest.raises(FeedSourceMismatch):
        helper(db, *args)
