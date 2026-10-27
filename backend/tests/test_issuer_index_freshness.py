"""An old prepared transcript must not pass a failed/latest-directory check."""
from datetime import timedelta

import pytest
from sqlalchemy import select

from app.services import issuer_discovery as discovery, issuer_transcripts as issuer
from app.services import direct_feed_store as store
from app.services.direct_feed_store import DirectFeedDocument, DirectFeedRevision
from test_issuer_discovery import setup, prepare, NOW


def ready(db):
    prepare(db)
    # SQLAlchemy retains the column default callable from model declaration.
    # Set fixture receipt time explicitly rather than relying on a monkeypatch.
    for revision in db.scalars(select(DirectFeedRevision)):
        revision.fetched_at = NOW
    db.commit()


@pytest.mark.parametrize('state', ['failed', 'pending', 'quarantined'])
def test_failed_current_index_cannot_serve_prepared_previous_transcript(setup, monkeypatch, state):
    monkeypatch.setattr(store, 'utcnow', lambda: NOW)
    ready(setup)
    assert issuer.prepared_transcript(setup, 'RDDT', observed_by=NOW)['status'] == 'prepared'
    row = setup.scalar(select(DirectFeedDocument).where(DirectFeedDocument.source_key == 'RDDT:reports:2026'))
    row.status = state
    setup.commit()
    result = issuer.prepared_transcript(setup, 'RDDT', observed_by=NOW)
    assert result['status'] == 'unavailable'
    assert result['reason'] == 'latest_issuer_index_not_verified'


@pytest.mark.parametrize('delta', [timedelta(hours=-48, seconds=-1), timedelta(seconds=1), None])
def test_stale_future_unchecked_current_index_is_not_fresh(setup, monkeypatch, delta):
    monkeypatch.setattr(store, 'utcnow', lambda: NOW)
    ready(setup)
    row = setup.scalar(select(DirectFeedDocument).where(DirectFeedDocument.source_key == 'RDDT:reports:2026'))
    row.checked_at = NOW + delta if delta is not None else None
    setup.commit()
    result = issuer.prepared_transcript(setup, 'RDDT', observed_by=NOW)
    assert result['status'] == 'unavailable'
    assert result['reason'] == 'latest_issuer_index_not_fresh'


def test_unchanged_old_original_with_fresh_check_remains_usable(setup, monkeypatch):
    monkeypatch.setattr(store, 'utcnow', lambda: NOW)
    ready(setup)
    before = issuer.prepared_transcript(setup, 'RDDT', observed_by=NOW)
    later = NOW + timedelta(days=3)
    for row in setup.scalars(select(DirectFeedDocument).where(DirectFeedDocument.feed == discovery.INDEX_FEED)):
        row.checked_at = later
    setup.commit()
    assert issuer.prepared_transcript(setup, 'RDDT', observed_by=later) == before

@pytest.mark.parametrize('change', ['missing', 'url', 'metadata', 'bytes', 'revision_missing', 'first_future'])
def test_current_directory_original_and_identity_are_required(setup, monkeypatch, change):
    import json
    from app.clients.direct_sources import DirectSourceError
    monkeypatch.setattr(store, 'utcnow', lambda: NOW)
    ready(setup)
    row = setup.scalar(select(DirectFeedDocument).where(DirectFeedDocument.source_key == 'RDDT:reports:2026'))
    revision = setup.scalar(select(DirectFeedRevision).where(DirectFeedRevision.document_id == row.id,
                                                           DirectFeedRevision.content_hash == row.content_hash))
    if change == 'missing':
        setup.delete(row)
    elif change == 'url':
        row.source_url = 'https://example.org/unverified'
    elif change == 'metadata':
        metadata = json.loads(row.metadata_json)
        metadata['symbol'] = 'MSFT'
        row.metadata_json = json.dumps(metadata)
    elif change == 'bytes':
        revision.source_bytes += b' '
    elif change == 'revision_missing':
        setup.delete(revision)
    else:
        revision.fetched_at = NOW + timedelta(seconds=1)
    setup.commit()
    if change in {'missing', 'first_future'}:
        result = issuer.prepared_transcript(setup, 'RDDT', observed_by=NOW)
        assert result['status'] == 'unavailable'
    else:
        with pytest.raises(DirectSourceError):
            issuer.prepared_transcript(setup, 'RDDT', observed_by=NOW)


def test_freshness_boundary_and_naive_observation(setup, monkeypatch):
    from app.clients.direct_sources import DirectSourceError
    monkeypatch.setattr(store, 'utcnow', lambda: NOW)
    ready(setup)
    assert issuer.prepared_transcript(setup, 'RDDT', observed_by=NOW + timedelta(hours=48))['status'] == 'prepared'
    with pytest.raises(DirectSourceError, match='timezone'):
        issuer.prepared_transcript(setup, 'RDDT', observed_by=NOW.replace(tzinfo=None))


def test_failed_directory_stops_research_without_fmp_or_new_records(setup, monkeypatch):
    from app.models import Security, ResearchSourceDocument, ResearchEvidenceEvent, EmailDelivery
    from app.services import operational_intelligence as operational
    from app.services.provider_usage import ProviderUnavailable
    from sqlalchemy import func
    monkeypatch.setattr(store, 'utcnow', lambda: NOW)
    ready(setup)
    security=Security(symbol='RDDT',name='Reddit',asset_class='stock')
    setup.add(security);setup.commit()
    index=setup.scalar(select(DirectFeedDocument).where(DirectFeedDocument.source_key=='RDDT:reports:2026'))
    index.status='failed';setup.commit()
    monkeypatch.setenv('TRANSCRIPT_PROVIDER','issuer')
    monkeypatch.setenv('ISSUER_TRANSCRIPT_PUBLISH_SINCE','2026-10-01')
    monkeypatch.setenv('RESEARCH_TRANSCRIPT_ANALYSIS_ENABLED','true')
    monkeypatch.setenv('FMP_PROVIDER_DISABLED','true')
    def forbidden(*a,**kw):pytest.fail('Unexpected legacy provider or extraction')
    monkeypatch.setattr(operational,'_fmp_rows',forbidden)
    monkeypatch.setattr(operational,'extract_document_events',forbidden)
    with pytest.raises(ProviderUnavailable,match='latest_issuer_index_not_verified'):
        operational._ingest_latest_transcript(setup,security=security)
    assert not setup.new and not setup.dirty
    for model in (ResearchSourceDocument, ResearchEvidenceEvent, EmailDelivery):
        assert setup.scalar(select(func.count()).select_from(model))==0
