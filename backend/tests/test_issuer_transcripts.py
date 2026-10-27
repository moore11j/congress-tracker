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
        # These reader/consumer cases need an actual fresh current directory,
        # not an implicit bypass of discovery health via a static transcript.
        from app.services import issuer_discovery as discovery
        company=next(c for c in json.loads(issuer.REGISTRY.read_bytes()) if c['symbol']=='MSFT')
        index_metadata=discovery.index_metadata(company,None)
        index_raw=('<main><h1>Microsoft Investor Relations</h1><a href="'+metadata['url']+'">Webcast</a></main>').encode()
        index_row=discover(db,discovery.INDEX_FEED,index_metadata)
        candidates=discovery.parse_index(index_raw,company,None,datetime.now(timezone.utc))
        record_document(db,index_row,index_raw,index_raw.decode(),{'candidates':candidates});db.commit()
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
    assert issuer.prepared_transcript(db,'MSFT',observed_by=datetime(2026,1,1,tzinfo=timezone.utc))['reason']=='latest_issuer_index_not_fresh'
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


def test_unchanged_transcript_in_new_html_keeps_first_receipt_without_duplicate_research(setup):
    db,security_id,row_id,metadata,raw=setup
    first=issuer.prepare_research_document(db,security_id=security_id,publish_since=date(2026,7,29));db.commit()
    row=db.get(DirectFeedDocument,row_id);original_receipt=row.reconciliation_json
    revised=raw.replace(b'<main>',b'<script>new request identifier</script><main>')
    text,parsed,holds=parse_document('issuer_earnings',revised,metadata);assert not holds
    record_document(db,row,revised,text,parsed);db.commit()
    saved=snapshot(db)
    second=issuer.prepare_research_document(db,security_id=security_id,publish_since=date(2026,7,29));db.commit()
    assert not second['created'] and second['document'].id==first['document'].id
    assert row.reconciliation_json==original_receipt and snapshot(db)==saved
    old_revision=db.get(DirectFeedRevision,json.loads(original_receipt)['issuer_transcript_research']['source_revision_id'])
    old_revision.source_bytes=b'changed original';db.commit()
    with pytest.raises(ValueError,match='Original issuer publication evidence changed'):
        issuer.prepare_research_document(db,security_id=security_id,publish_since=date(2026,7,29))


def test_official_link_provenance_is_preserved_and_cannot_be_rebound(setup):
    db, security_id, row_id, metadata, raw = setup
    metadata.update(source_page_url='https://www.microsoft.com/en-us/Investor', source_page_sha256='a'*64)
    raw += b'\n'  # New receipt, rather than rewriting the fixture's immutable revision.
    row = db.get(DirectFeedDocument, row_id)
    row.metadata_json = dumps(metadata)
    text, parsed, reasons = parse_document('issuer_earnings', raw, metadata)
    record_document(db, row, raw, text, parsed)
    db.commit()
    first = issuer.prepare_research_document(db, security_id=security_id, publish_since=date(2026,7,29))
    db.commit()
    assert first['publication_evidence']['source_page_sha256'] == 'a'*64
    assert first['source']['source_page_url'] == metadata['source_page_url']
    metadata['source_page_sha256'] = 'b'*64
    row.metadata_json = dumps(metadata)
    revised = raw+b'\n'
    text, parsed, reasons = parse_document('issuer_earnings', revised, metadata)
    record_document(db, row, revised, text, parsed)
    db.commit()
    with pytest.raises(ValueError, match='identity or boundary'):
        issuer.prepare_research_document(db, security_id=security_id, publish_since=date(2026,7,29))


def _historical_fmp(db, security_id):
    from app.services.research_evidence import EVIDENCE_PROCESSING_VERSION
    document, _ = upsert_source_document(db, security_id=security_id,
        document_type='earnings_transcript', source_provider='fmp',
        external_id=f'earnings_transcript:{security_id}:2026:Q4',
        content='The original provider rendering is deliberately different.',
        filing_type='Q4-2026', published_at=datetime(2026,7,29,tzinfo=timezone.utc))
    document.processing_status='processed'
    document.processing_version=EVIDENCE_PROCESSING_VERSION
    db.commit()
    return document


def test_historical_different_text_retains_original_document_and_repeat_state(setup):
    db, security_id, row_id, metadata, raw = setup
    document = _historical_fmp(db, security_id)
    before = {c.name: getattr(document,c.name) for c in document.__table__.columns}
    value = issuer.prepare_research_document(db,security_id=security_id,publish_since=date(2026,10,10))
    db.commit()
    assert value['status']=='retained_history' and 'source_text' not in value
    assert value['source']['text_sha256']!=document.content_hash
    assert value['publication_evidence']['historical_document']['source_provider']=='fmp'
    assert {c.name: getattr(document,c.name) for c in document.__table__.columns}==before
    saved=snapshot(db)
    again=issuer.prepare_research_document(db,security_id=security_id,publish_since=date(2026,10,10))
    db.commit()
    assert again['publication_evidence']==value['publication_evidence'] and snapshot(db)==saved
    assert db.scalar(select(func.count()).select_from(ResearchSourceDocument))==1
    assert db.scalar(select(func.count()).select_from(ResearchEvidenceEvent))==0
    assert db.scalar(select(func.count()).select_from(EmailDelivery))==0


@pytest.mark.parametrize('case',['pending','date','identity','provider','period','duplicate','undated'])
def test_historical_conflicts_do_not_create_receipt_or_rewrite_research(setup,case):
    db,security_id,*_=setup
    document=_historical_fmp(db,security_id)
    if case=='pending':document.processing_status='pending'
    if case=='date':document.published_at=datetime(2026,7,28,tzinfo=timezone.utc)
    if case=='identity':document.external_id='unverified-identity'
    if case=='provider':document.source_provider='other'
    if case=='period':document.filing_type='Q3-2026'
    if case=='undated':document.published_at=None
    if case=='duplicate':
        upsert_source_document(db,security_id=security_id,document_type='earnings_transcript',
            source_provider='other',external_id='another-quarter-record',content='Other text',filing_type='Q4-2026')
    db.commit();before=snapshot(db)
    value=issuer.prepare_research_document(db,security_id=security_id,publish_since=date(2026,10,10))
    db.commit()
    assert value['status']=='held' and snapshot(db)==before


@pytest.mark.parametrize('case',['content','processing_version','boundary_forward','boundary_backward'])
def test_established_history_cannot_silently_change(setup,case):
    db,security_id,*_=setup
    document=_historical_fmp(db,security_id)
    issuer.prepare_research_document(db,security_id=security_id,publish_since=date(2026,10,10));db.commit()
    boundary=date(2026,10,10)
    if case=='content':document.content_hash='a'*64
    if case=='processing_version':document.processing_version='changed'
    if case=='boundary_forward':boundary=date(2026,10,11)
    if case=='boundary_backward':boundary=date(2026,7,29)
    db.commit()
    with pytest.raises(ValueError):
        issuer.prepare_research_document(db,security_id=security_id,publish_since=boundary)


def test_historical_consumer_reuses_event_matching_without_extraction_or_fmp_fallback(setup,monkeypatch):
    from app.services import operational_intelligence as operational
    from app.models import ResearchSourceCoverage
    db,security_id,*_=setup
    document=_historical_fmp(db,security_id)
    monkeypatch.setenv('TRANSCRIPT_PROVIDER','issuer')
    monkeypatch.setenv('ISSUER_TRANSCRIPT_PUBLISH_SINCE','2026-10-10')
    monkeypatch.setenv('RESEARCH_TRANSCRIPT_ANALYSIS_ENABLED','true')
    monkeypatch.setenv('FMP_PROVIDER_DISABLED','true')
    def forbidden(*a,**k):pytest.fail('Historical baseline attempted extraction, matching or FMP')
    for name in ['extract_document_events','_fmp_rows']:
        monkeypatch.setattr(operational,name,forbidden)
    matched=[]
    monkeypatch.setattr(operational,'_match_document_events',lambda db,doc_id: matched.append(doc_id) or 0)
    for _ in range(2):
        result=operational.refresh_operational_intelligence(db,security_id=security_id,source_types={'earnings_transcript'})
        assert (result['documents'],result['events'],result['matches'],result['extraction_attempts'])==(0,0,0,0)
    coverage=db.get(ResearchSourceCoverage,(security_id,'issuer_transcript'))
    assert matched==[document.id,document.id]
    assert coverage.status=='partial' and coverage.failure_reason=='historical_transcript_retained'
    public=operational.ticker_operational_intelligence(db,security=db.get(Security,security_id))
    row=next(r for r in public['coverage'] if r['source_type']=='earnings_transcript')
    assert row['historical_provider']=='fmp' and row['provider']=='issuer' and row['complete'] is False
    assert document.source_provider=='fmp'
    assert db.scalar(select(func.count()).select_from(EmailDelivery))==0


def test_historical_transport_revision_keeps_first_receipt_and_rejects_tampering(setup):
    db,security_id,row_id,metadata,raw=setup
    _historical_fmp(db,security_id)
    first=issuer.prepare_research_document(db,security_id=security_id,publish_since=date(2026,10,10));db.commit()
    row=db.get(DirectFeedDocument,row_id)
    revised=raw.replace(b'<main>',b'<script>new tracking identifier</script><main>')
    text,parsed,holds=parse_document('issuer_earnings',revised,metadata)
    assert not holds
    record_document(db,row,revised,text,parsed);db.commit()
    before=snapshot(db)
    second=issuer.prepare_research_document(db,security_id=security_id,publish_since=date(2026,10,10));db.commit()
    assert first['publication_evidence']==second['publication_evidence'] and snapshot(db)==before
    original=db.get(DirectFeedRevision,first['publication_evidence']['source_revision_id'])
    original.source_bytes=b'changed';db.commit()
    with pytest.raises(ValueError,match='Original issuer publication evidence changed'):
        issuer.prepare_research_document(db,security_id=security_id,publish_since=date(2026,10,10))


def test_new_period_after_retained_history_creates_only_one_issuer_document(setup,monkeypatch):
    db,security_id,row_id,metadata,raw=setup
    old=_historical_fmp(db,security_id)
    boundary=date(2026,9,1)
    first=issuer.prepare_research_document(db,security_id=security_id,publish_since=boundary)
    db.commit();assert first['status']=='retained_history'
    historical_hash=old.content_hash
    current={**metadata,'key':'MSFT:earnings_transcript:2027:Q1','fiscal_year':2027,'fiscal_quarter':1,
        'published_at':'2026-10-10','period_pattern':r'FY2027 Q1','publication_pattern':r'October 10, 2026',
        'url':'https://www.microsoft.com/en-us/investor/events/fy-2027/earnings-fy-2027-q1'}
    monkeypatch.setattr(issuer,'_reviewed_latest',lambda symbol:current)
    current_raw=raw.replace(b'FY2026 Q4',b'FY2027 Q1').replace(b'July 29, 2026',b'October 10, 2026')
    row=discover(db,'issuer_earnings',current)
    content,parsed,holds=parse_document('issuer_earnings',current_raw,current)
    assert not holds
    record_document(db,row,current_raw,content,parsed);db.commit()
    new=issuer.prepare_research_document(db,security_id=security_id,publish_since=boundary);db.commit()
    assert new['status']=='prepared' and new['created']
    assert new['document'].source_provider=='issuer' and new['document'].id!=old.id
    assert new['source_text']==content
    before=snapshot(db)
    repeat=issuer.prepare_research_document(db,security_id=security_id,publish_since=boundary);db.commit()
    assert not repeat['created'] and snapshot(db)==before
    assert old.source_provider=='fmp' and old.content_hash==historical_hash
    assert db.scalar(select(func.count()).select_from(ResearchSourceDocument))==2
