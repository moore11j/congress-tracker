from datetime import date,datetime,timezone
import hashlib,json
import pytest
from sqlalchemy import create_engine,select,func
from sqlalchemy.orm import Session
from app.db import Base
from app.models import Security,ResearchSourceDocument,ResearchEvidenceEvent,EmailDelivery
from app.services import issuer_transcripts as issuer
from app.services.direct_feed_collection import parse_document
from app.services.direct_feed_store import discover,record_document,DirectFeedDocument,DirectFeedRevision,dumps
from app.services.research_evidence import upsert_source_document


@pytest.fixture
def setup(monkeypatch):
    def forbidden(*a,**k):pytest.fail('Transport or delivery during prepared transcript read')
    monkeypatch.setattr('requests.sessions.Session.request',forbidden)
    monkeypatch.setattr('httpx.Client.send',forbidden)
    metadata={'key':'MSFT:earnings_transcript:2026:Q4','symbol':'MSFT','company_website':'https://www.microsoft.com/',
        'investor_website':'https://www.microsoft.com/en-us/Investor','url':'https://www.microsoft.com/en-us/investor/events/fy-2026/earnings-fy-2026-q4',
        'document_type':'earnings_transcript','fiscal_year':2026,'fiscal_quarter':4,'published_at':'2026-07-29',
        'period_pattern':r'FY2026 Q4','company_pattern':r'Microsoft','publication_pattern':r'July 29, 2026'}
    monkeypatch.setattr(issuer,'_reviewed_latest',lambda symbol:metadata if symbol=='MSFT' else None)
    raw=('<html><main>Microsoft earnings call transcript FY2026 Q4 July 29, 2026 Q&amp;A '+('Company earnings evidence and answers. '*40)+'</main></html>').encode()
    engine=create_engine('sqlite:///:memory:');Base.metadata.create_all(engine)
    with Session(engine) as db:
        security=Security(symbol='MSFT',name='Microsoft',asset_class='stock');db.add(security);db.commit()
        row=discover(db,'issuer_earnings',metadata)
        text,parsed,reasons=parse_document('issuer_earnings',raw,metadata);assert not reasons
        record_document(db,row,raw,text,parsed);db.commit()
        yield db,security.id,row.id,metadata,raw
    engine.dispose()


def snapshot(db):
    return dumps([[t.name,sorted([dict(r._mapping) for r in db.execute(select(t))],key=dumps)] for t in Base.metadata.sorted_tables])


def test_prepared_contract_preserves_date_precision_fiscal_identity_and_repeat(setup):
    db,security_id,row_id,metadata,raw=setup
    before=snapshot(db);value=issuer.prepared_transcript(db,'MSFT')
    assert value['status']=='prepared' and value['has_qa']
    assert value['year']==2026 and value['quarter']==4
    assert value['published_date']=='2026-07-29' and value['publication_precision']=='date'
    assert value['source_sha256']==hashlib.sha256(raw).hexdigest()
    assert issuer.prepared_transcript(db,'MSFT')==value and snapshot(db)==before


def test_research_preparation_repeats_without_duplicate_documents_events_or_delivery(setup):
    db,security_id,*_=setup
    first=issuer.prepare_research_document(db,security_id=security_id,publish_since=date(2026,7,29));db.commit()
    assert first['created'] and first['document'].published_at is None
    saved=snapshot(db)
    second=issuer.prepare_research_document(db,security_id=security_id,publish_since=date(2026,7,29));db.commit()
    assert not second['created'] and second['document'].id==first['document'].id
    assert snapshot(db)==saved
    assert db.scalar(select(func.count()).select_from(ResearchSourceDocument))==1
    assert db.scalar(select(func.count()).select_from(ResearchEvidenceEvent))==0
    assert db.scalar(select(func.count()).select_from(EmailDelivery))==0


@pytest.mark.parametrize('case',['exact','different','duplicate'])
def test_existing_fmp_period_reuses_exact_source_or_holds_without_rewriting_history(setup,case):
    db,security_id,*_=setup
    source=issuer.prepared_transcript(db,'MSFT')
    content=source['content'] if case!='different' else 'Different original provider rendering'
    document,_=upsert_source_document(db,security_id=security_id,document_type='earnings_transcript',source_provider='fmp',
        external_id=f'earnings_transcript:{security_id}:2026:Q4',content=content,filing_type='Q4-2026',published_at=datetime(2026,7,29,tzinfo=timezone.utc))
    if case=='duplicate':
        upsert_source_document(db,security_id=security_id,document_type='earnings_transcript',source_provider='other',external_id='same-quarter',content=content,filing_type='Q4-2026')
    db.commit();original=(document.source_provider,document.published_at,document.content_hash);before=snapshot(db)
    result=issuer.prepare_research_document(db,security_id=security_id,publish_since=date(2026,7,29));db.commit()
    if case=='exact':
        assert not result['created'] and result['document'].id==document.id
    else:
        assert result['status']=='held' and result['reason']=='existing_period_requires_reconciliation'
        assert snapshot(db)==before
    assert (document.source_provider,document.published_at,document.content_hash)==original


@pytest.mark.parametrize('case',['bytes','parsed','revision_text'])
def test_changed_evidence_fails_before_canonical_write(setup,case):
    db,security_id,row_id,metadata,raw=setup
    row=db.get(DirectFeedDocument,row_id);revision=db.scalar(select(DirectFeedRevision).where(DirectFeedRevision.document_id==row_id))
    if case=='bytes':revision.source_bytes=b'tampered'
    if case=='parsed':row.parsed_json='{}'
    if case=='revision_text':revision.source_text+='tampered'
    db.commit()
    with pytest.raises(ValueError):issuer.prepare_research_document(db,security_id=security_id,publish_since=date(2026,7,29))
    db.rollback();assert db.scalar(select(func.count()).select_from(ResearchSourceDocument))==0


def test_missing_future_and_before_cutoff_are_explicit_unavailable_or_held(setup):
    db,security_id,*_=setup
    assert issuer.prepared_transcript(db,'AAPL')['reason']=='issuer_not_configured'
    assert issuer.prepared_transcript(db,'MSFT',observed_by=datetime(2026,1,1,tzinfo=timezone.utc))['reason']=='source_not_yet_observed'
    result=issuer.prepare_research_document(db,security_id=security_id,publish_since=date(2026,7,30))
    assert result['reason']=='before_issuer_publication_boundary'
    assert db.scalar(select(func.count()).select_from(ResearchSourceDocument))==0


def test_publication_boundary_cannot_change_silently(setup):
    db,security_id,*_=setup
    issuer.prepare_research_document(db,security_id=security_id,publish_since=date(2026,7,29));db.commit()
    with pytest.raises(ValueError,match='boundary changed'):
        issuer.prepare_research_document(db,security_id=security_id,publish_since=date(2026,7,28))
    db.rollback()


def test_selected_issuer_research_works_with_fmp_disabled_and_scopes_coverage(setup,monkeypatch):
    from app.services import operational_intelligence as operational
    from app.services.research_evidence import EVIDENCE_PROCESSING_VERSION
    from app.models import ResearchSourceCoverage
    db,security_id,*_=setup
    monkeypatch.setenv('TRANSCRIPT_PROVIDER','issuer')
    monkeypatch.setenv('ISSUER_TRANSCRIPT_PUBLISH_SINCE','2026-07-29')
    monkeypatch.setenv('RESEARCH_TRANSCRIPT_ANALYSIS_ENABLED','true')
    monkeypatch.setenv('FMP_PROVIDER_DISABLED','true')
    monkeypatch.setattr(operational,'_fmp_rows',lambda *a,**k:pytest.fail('Selected issuer used FMP'))
    monkeypatch.setattr(operational,'_match_document_events',lambda *a,**k:0)
    calls=[]
    def extract(db,*,document,source_text,consume_call):
        calls.append(document.id)
        document.processing_status='processed';document.processing_version=EVIDENCE_PROCESSING_VERSION
        db.commit()
        return {'status':'processed','events_written':0}
    monkeypatch.setattr(operational,'extract_document_events',extract)
    first=operational.refresh_operational_intelligence(db,security_id=security_id,source_types={'earnings_transcript'})
    assert first['documents']==1 and len(calls)==1
    second=operational.refresh_operational_intelligence(db,security_id=security_id,source_types={'earnings_transcript'})
    assert second['documents']==0 and len(calls)==1
    coverage=db.get(ResearchSourceCoverage,(security_id,'issuer_transcript'))
    assert coverage.status=='ready' and coverage.documents_seen==1
    public=operational.ticker_operational_intelligence(db,security=db.get(Security,security_id))
    row=next(r for r in public['coverage'] if r['source_type']=='earnings_transcript')
    assert row['provider']=='issuer' and row['complete'] is False and row['status']=='ready'
    assert db.scalar(select(func.count()).select_from(EmailDelivery))==0


def test_issuer_missing_cutoff_and_unknown_company_cannot_call_legacy_provider(setup,monkeypatch):
    from app.services import operational_intelligence as operational
    from app.services.provider_usage import ProviderUnavailable
    db,security_id,*_=setup
    monkeypatch.setenv('TRANSCRIPT_PROVIDER','issuer');monkeypatch.setenv('RESEARCH_TRANSCRIPT_ANALYSIS_ENABLED','true')
    monkeypatch.delenv('ISSUER_TRANSCRIPT_PUBLISH_SINCE',raising=False)
    monkeypatch.setattr(operational,'_fmp_rows',lambda *a,**k:pytest.fail('Unexpected FMP fallback'))
    with pytest.raises(ProviderUnavailable,match='boundary_missing'):
        operational._ingest_latest_transcript(db,security=db.get(Security,security_id))
    monkeypatch.setenv('ISSUER_TRANSCRIPT_PUBLISH_SINCE','2026-07-29')
    security=Security(symbol='AAPL',name='Apple',asset_class='stock');db.add(security);db.commit()
    with pytest.raises(ProviderUnavailable,match='issuer_not_configured'):
        operational._ingest_latest_transcript(db,security=security)
