from datetime import datetime, timedelta, timezone
import json

import pytest
from sqlalchemy import func, select

from app.models import Event, ResearchSourceDocument, ResearchEvidenceEvent, ResearchSourceCoverage, Security, EmailDelivery
from app.services.feed_source_control import FeedSourceMismatch
from app.services.sec_press_research import prepare_sec_research_document
from app.services.direct_feed_store import DirectFeedDocument
from app.services.research_evidence import extract_document_events, EVIDENCE_PROCESSING_VERSION
from app.services import operational_intelligence as operational
from test_sec_press_events import db, stage, activate
from test_research_evidence import Response


def prepare(db, doc):
    return prepare_sec_research_document(db, security_id=1, accession=doc.source_key)


def test_source_default_prevents_research_publication(db):
    doc, _ = stage(db)
    with pytest.raises(FeedSourceMismatch): prepare(db, doc)
    db.rollback()
    assert db.scalar(select(func.count()).select_from(ResearchSourceDocument)) == 0


def test_shared_event_and_research_identity_preserves_text_and_unknown_press_time(db):
    doc, day = stage(db); activate(db, day)
    result = prepare(db, doc); db.commit()
    research = result['document']; parsed = json.loads(doc.parsed_json)
    assert result['created'] and result['source_text'] == parsed['release']['text']
    assert research.content_hash == parsed['release']['text_sha256']
    assert research.source_provider == 'sec_edgar' and research.external_id == parsed['canonical_key']
    assert research.published_at is None and research.filing_type == '8-K'
    assert research.source_url == parsed['release']['url']
    assert not prepare(db, doc)['created']
    assert db.scalar(select(func.count()).select_from(ResearchSourceDocument)) == 1
    assert db.scalar(select(func.count()).select_from(Event)) == 1


def test_rollback_covers_both_research_and_alert_event(db):
    doc, day = stage(db); activate(db, day)
    prepare(db, doc); db.rollback()
    assert db.scalar(select(func.count()).select_from(ResearchSourceDocument)) == 0
    assert db.scalar(select(func.count()).select_from(Event)) == 0


def test_exact_legacy_research_document_is_reused_without_relabeling(db):
    doc, day = stage(db); activate(db, day)
    parsed = json.loads(doc.parsed_json)
    row = ResearchSourceDocument(id='legacy', security_id=1, document_type='press_release', source_provider='fmp',
        external_id='retained-id', title='Original headline', source_url='https://issuer.test/results',
        content_hash=parsed['release']['text_sha256'], published_at=datetime.now(timezone.utc)-timedelta(days=1),
        processing_status='processed', processing_version=EVIDENCE_PROCESSING_VERSION)
    db.add(row); db.commit()
    before = (row.id, row.source_provider, row.external_id, row.title, row.source_url, row.published_at, row.processing_status)
    result = prepare(db, doc); db.commit()
    assert result['document'].id == 'legacy' and not result['created']
    assert before == (row.id, row.source_provider, row.external_id, row.title, row.source_url, row.published_at, row.processing_status)
    assert db.scalar(select(func.count()).select_from(ResearchSourceDocument)) == 1


def test_old_filing_does_not_become_new_research_or_alert(db):
    doc, day = stage(db, day=datetime.now(timezone.utc).date()-timedelta(days=10)); activate(db, day)
    assert prepare(db, doc)['reason'] == 'outside_recent_publication_boundary'
    assert db.scalar(select(func.count()).select_from(ResearchSourceDocument)) == 0
    assert db.scalar(select(func.count()).select_from(Event)) == 0


def test_ambiguous_legacy_material_does_not_create_a_second_document(db):
    doc, day = stage(db); activate(db, day)
    db.add(ResearchSourceDocument(id='ambiguous', security_id=1, document_type='press_release',
        source_provider='fmp', external_id='existing', content_hash='different-text',
        published_at=None, processing_status='processed', processing_version=EVIDENCE_PROCESSING_VERSION))
    db.commit()
    assert prepare(db, doc)['reason'] == 'ambiguous_existing_material'
    assert db.scalar(select(func.count()).select_from(ResearchSourceDocument)) == 1
    assert db.scalar(select(func.count()).select_from(Event)) == 0


def test_selected_sec_source_blocks_legacy_research_writer(db):
    doc, day = stage(db); activate(db, day)
    with pytest.raises(FeedSourceMismatch):
        operational._ingest_article(db, security=db.get(Security, 1), document_type='press_release',
            item={'title':'Cached legacy title', 'summary':'Cached text may not become a second source.'})
    db.rollback()
    assert db.scalar(select(func.count()).select_from(ResearchSourceDocument)) == 0


def configure_worker(db, monkeypatch):
    doc, day = stage(db); activate(db, day)
    monkeypatch.setenv('RESEARCH_OPERATIONAL_INTELLIGENCE_ENABLED', '1')
    monkeypatch.setenv('RESEARCH_OPERATIONAL_MAX_EXTRACTIONS_PER_RUN', '10')
    monkeypatch.setattr(operational, 'claim_matching_enabled', lambda: False)
    def prepared_loader(**kwargs):
        assert kwargs.get('prepared_only') is True and not kwargs.get('force_refresh')
        return {'status':'ok', 'provider':'sec_edgar_earnings', 'coverage':{'complete':False},
            'items':[{'accession_number':doc.source_key, 'title':'Panel description must not be extracted'}]}
    monkeypatch.setattr(operational, 'get_press_releases', prepared_loader)
    calls = []
    answer = {'events':[{'category':'company_operations', 'event_type':'operational_milestone', 'subject':'Test',
        'metric':'reported results', 'direction':'positive', 'previous_text':None, 'current_text':None,
        'headline':'Test reports higher revenue and net income', 'summary':'Revenue and net income increased.',
        'source_passage_id':'passage_0', 'watch_item':None, 'confidence':'medium', 'materiality':'medium'}]}
    def extract(session, **kwargs):
        assert kwargs['source_text'] == json.loads(doc.parsed_json)['release']['text']
        return extract_document_events(session, **kwargs,
            request_sender=lambda: calls.append(True) or Response(json.dumps(answer)))
    monkeypatch.setattr(operational, 'extract_document_events', extract)
    return calls


def test_operational_worker_uses_actual_extractor_with_saved_sec_text_once(db, monkeypatch):
    calls = configure_worker(db, monkeypatch)
    first = operational.refresh_operational_intelligence(db, security_id=1, source_types={'press_release'})
    second = operational.refresh_operational_intelligence(db, security_id=1, source_types={'press_release'})
    assert (first['documents'], first['events'], first['extraction_attempts']) == (1, 1, 1)
    assert (second['documents'], second['events'], second['extraction_attempts']) == (0, 0, 0)
    assert len(calls) == 1
    event = db.scalar(select(ResearchEvidenceEvent))
    assert event.source_provider == 'sec_edgar' and event.published_at is None
    staged = db.scalar(select(DirectFeedDocument).where(DirectFeedDocument.feed == 'sec_earnings_release'))
    assert event.evidence_excerpt in json.loads(staged.parsed_json)['release']['text']
    assert event.source_locator.startswith('document_text:')
    view = operational.ticker_operational_intelligence(db, security=db.get(Security,1))
    assert view['catalysts'][0]['source_url'] == event.source_url
    assert view['catalysts'][0]['published_at'] is None
    assert view['catalysts'][0]['evidence_excerpt'] == event.evidence_excerpt
    coverage = view['coverage'][1]
    assert coverage['status'] == 'ready' and coverage['provider'] == 'sec_edgar_earnings'
    assert coverage['complete'] is False
    assert db.scalar(select(func.count()).select_from(Event)) == 1
    assert db.scalar(select(func.count()).select_from(EmailDelivery)) == 0


def test_budget_deferred_analysis_is_partial_and_resumes_without_second_source(db, monkeypatch):
    calls = configure_worker(db, monkeypatch)
    monkeypatch.setenv('RESEARCH_OPERATIONAL_MAX_EXTRACTIONS_PER_RUN', '0')
    operational.refresh_operational_intelligence(db, security_id=1, source_types={'press_release'})
    assert db.get(ResearchSourceCoverage, (1,'sec_earnings_release')).status == 'partial'
    assert calls == []
    monkeypatch.setenv('RESEARCH_OPERATIONAL_MAX_EXTRACTIONS_PER_RUN', '10')
    result = operational.refresh_operational_intelligence(db, security_id=1, source_types={'press_release'})
    assert result['documents'] == 0 and result['events'] == 1 and len(calls) == 1
    assert db.scalar(select(func.count()).select_from(ResearchSourceDocument)) == 1


def test_provider_failure_preserves_source_for_retry_and_reports_unavailable(db, monkeypatch):
    calls = configure_worker(db, monkeypatch)
    working = operational.extract_document_events
    def failed(session, **kwargs):
        response = Response('{}'); response.status_code = 503
        return extract_document_events(session, **kwargs, request_sender=lambda: response)
    monkeypatch.setattr(operational, 'extract_document_events', failed)
    operational.refresh_operational_intelligence(db, security_id=1, source_types={'press_release'})
    assert db.get(ResearchSourceCoverage, (1,'sec_earnings_release')).status == 'unavailable'
    assert db.scalar(select(ResearchSourceDocument)).processing_status == 'failed'
    monkeypatch.setattr(operational, 'extract_document_events', working)
    result = operational.refresh_operational_intelligence(db, security_id=1, source_types={'press_release'})
    assert result['documents'] == 0 and result['events'] == 1 and len(calls) == 1
    assert db.scalar(select(func.count()).select_from(ResearchSourceDocument)) == 1
    assert db.scalar(select(func.count()).select_from(Event)) == 1


def test_retired_source_coverage_and_old_failures_are_not_reused_for_sec(db, monkeypatch):
    configure_worker(db, monkeypatch)
    db.add(ResearchSourceCoverage(security_id=1, source_type='press_release', status='ready', documents_seen=99,
                                 last_success_at=datetime.now(timezone.utc)))
    db.add(ResearchSourceDocument(id='old-failed', security_id=1, document_type='press_release', source_provider='fmp',
        external_id='old-failed', content_hash='old-text', processing_status='failed', processing_version='old',
        published_at=datetime.now(timezone.utc)-timedelta(days=50)))
    db.commit()
    assert operational.ticker_operational_intelligence(db, security=db.get(Security,1))['coverage'][1]['status'] == 'not_checked'
    operational.refresh_operational_intelligence(db, security_id=1, source_types={'press_release'})
    coverage = operational.ticker_operational_intelligence(db, security=db.get(Security,1))['coverage'][1]
    assert coverage['status'] == 'ready' and coverage['documents_seen'] == 1
    assert db.get(ResearchSourceDocument, 'old-failed').processing_status == 'failed'
