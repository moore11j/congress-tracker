from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import socket

import pytest
from sqlalchemy import create_engine, select, func, event
from sqlalchemy.orm import Session
from app.db import Base
from app.models import Security, ResearchSourceDocument, ResearchEvidenceEvent
from app.clients.direct_sources import DirectSourceError
from app.services import issuer_discovery as discovery, issuer_transcripts as issuer, issuer_unilever_discovery as policy
from app.services import direct_feed_store as store
from app.services.direct_feed_store import DirectFeedDocument, DirectFeedRevision
from test_issuer_pdf import pdf

NOW=datetime(2026,10,15,tzinfo=timezone.utc)
def company(): return deepcopy(next(c for c in json.loads(issuer.REGISTRY.read_bytes()) if c['symbol']=='UL'))

def index(year=2026,quarter=2,day='28 July 2026'):
    label={1:'Q1',2:'Q2 and Half-Year',3:'Q3',4:'Q4 and Full-Year'}[quarter]
    kind='Trading Statement' if quarter in (1,3) else 'Results'
    return (f'<html><section class="uco-c-focal-section"><h2>{label} {year} {kind}</h2>'
            f'<p>Unilever released its {label} {kind} on {day}.</p>'
            f'<a href="/files/unilever-q{quarter}-{year}-transcript.pdf">Q{quarter} {year} Video Transcript (PDF 486.08 KB)</a>'
            '</section></html>').encode()

def transcript(year=2026,quarter=2,day='28th July 2026',qa=True,pages=1):
    title={1:f'Q1 {year} Trading Statement',2:f'Q2 & H1 {year} Results',3:f'Q3 {year} Trading Statement',4:f'Q4 & Full Year {year} Results'}[quarter]
    return pdf(f'Unilever {title} London, 8:00am, Tuesday {day} FERNANDO FERNANDEZ: '
               +'Synthetic earnings financial results discussion. '*40
               +('JEMMA SPALTON: Welcome. Our first question comes from an analyst. '+ 'Synthetic Q and A response. '*40 if qa else ''),pages=pages)

@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    monkeypatch.setenv('FMP_PROVIDER_ENABLED','false');monkeypatch.setenv('FMP_PROVIDER_DISABLED','true')
    calls=[]
    def deny(*a,**kw):calls.append(True);raise AssertionError('Unexpected network')
    monkeypatch.setattr(socket.socket,'connect',deny)
    yield
    assert not calls

@pytest.fixture
def db(monkeypatch):
    monkeypatch.setattr(discovery,'utcnow',lambda:NOW);monkeypatch.setattr(store,'utcnow',lambda:NOW)
    def stamp(mapper, connection, target): target.fetched_at=NOW
    event.listen(DirectFeedRevision, 'before_insert', stamp)
    engine=create_engine('sqlite:///:memory:');Base.metadata.create_all(engine)
    with Session(engine) as session:yield session
    engine.dispose()
    event.remove(DirectFeedRevision, 'before_insert', stamp)

class Client:
    issuer_document_urls=frozenset()
    def __init__(self,db):self.db,self.calls,self.raw,self.body=db,[],index(),transcript()
    def get(self,url):
        assert not self.db.in_transaction();self.calls.append(url)
        if url==policy.INDEX_URL:return self.raw
        assert url in self.issuer_document_urls
        return self.body

def snapshot(db):return {t.name:[dict(r._mapping) for r in db.execute(select(t))] for t in Base.metadata.sorted_tables}

@pytest.mark.parametrize('year,quarter,day,pdfday',[(2026,1,'30 April 2026','30th April 2026'),(2026,2,'28 July 2026','28th July 2026'),(2026,3,'9 October 2026','9th October 2026'),(2025,4,'12 February 2026','12th February 2026')])
def test_quarter_and_year_end_call_identity(year,quarter,day,pdfday):
    metadata=policy.parse_index(index(year,quarter,day),company(),NOW)[0]
    resolved,text,parsed=policy.parse_candidate(transcript(year,quarter,pdfday),metadata,NOW)
    assert parsed['has_qa'] and parsed['canonical_key']==f'UL:earnings_transcript:{year}:Q{quarter}'
    assert resolved==metadata and metadata['published_at'].startswith('2026-')

@pytest.mark.parametrize('old,new',[
 (b'/files/unilever-q2-2026-transcript.pdf',b'https://evil.example/unilever-q2-2026-transcript.pdf'),
 (b'/files/unilever-q2-2026-transcript.pdf',b'http://www.unilever.com/files/unilever-q2-2026-transcript.pdf'),
 (b'/files/unilever-q2-2026-transcript.pdf',b'https://user@www.unilever.com/files/unilever-q2-2026-transcript.pdf'),
 (b'transcript.pdf',b'transcript.pdf?x'),(b'transcript.pdf',b'transcript.pdf#x'),
 (b'/files/unilever-q2-',b'/files/unilever-q1-'),
 (b'Q2 2026 Video',b'Q1 2026 Video'),(b'28 July 2026',b'28 November 2026'),
 (b'28 July 2026',b'31 February 2026'),(b'28 July 2026',b'28 May 2026'),
 (b'Q2 and Half-Year 2026',b'Q2 and Full-Year 2026'),
 (b'Unilever released',b'Other company released'),
 (b'Video Transcript',b'Video Presentation'),
 (b'<h2>',b'<h1>'),(b'<section',b'<div'),
])
def test_invalid_or_unreviewed_index_cannot_grant_download(old,new):
    with pytest.raises(DirectSourceError):policy.parse_index(index().replace(old,new),company(),NOW)

@pytest.mark.parametrize('raw',[b'',b'<html/>',b'x'*2_000_001,index()+index()], ids=['empty','missing','oversized','duplicate'])
def test_empty_oversized_or_duplicate_period_is_held(raw):
    with pytest.raises(DirectSourceError):policy.parse_index(raw,company(),NOW)

@pytest.mark.parametrize('case',['company','quarter','date','qa','date_only_in_body','page_limit'])
def test_pdf_independently_proves_identity_date_and_full_qa(case):
    m=policy.parse_index(index(),company(),NOW)[0]
    body=transcript()
    if case=='company':m['company_pattern']='Other Company'
    if case=='quarter':body=transcript(quarter=1)
    if case=='date':body=transcript(day='29th July 2026')
    if case=='qa':body=transcript(qa=False)
    if case=='date_only_in_body':body=pdf('Unilever Q2 & H1 2026 Results London, Unknown FERNANDO FERNANDEZ: '+'earnings results '*100+'28th July 2026 JEMMA SPALTON: Our first question comes from analyst.')
    if case=='page_limit':body=transcript(pages=61)
    with pytest.raises(DirectSourceError):policy.parse_candidate(body,m,NOW)

def test_long_reviewed_original_capacity_is_issuer_specific():
    m=policy.parse_index(index(),company(),NOW)[0]
    assert policy.parse_candidate(transcript(pages=45),m,NOW)[2]['has_qa']
    from app.clients.direct_sources import parse_issuer_material
    m.pop('discovery_kind')
    with pytest.raises(DirectSourceError,match='page limit'):parse_issuer_material(transcript(pages=45),m)

def test_collector_reader_canonical_repeat_and_source_tamper(db):
    client=Client(db);c=company()
    result=discovery.collect_discovery(db,client,[c],limit=20,recheck_hours=1)
    assert result['attempted']==result['processed']==2 and not result['errors']
    source=issuer.prepared_transcript(db,'UL',observed_by=NOW)
    assert source['quarter']==2 and source['has_qa'] and source['published_date']=='2026-07-28'
    security=Security(symbol='UL',name='Synthetic Unilever',asset_class='stock');db.add(security);db.commit()
    assert issuer.prepare_research_document(db,security_id=security.id,publish_since=date(2026,7,1))['created'];db.commit()
    before=snapshot(db)
    assert not issuer.prepare_research_document(db,security_id=security.id,publish_since=date(2026,7,1))['created'];db.commit()
    assert discovery.collect_discovery(db,client,[c],limit=2,recheck_hours=1)['attempted']==0
    assert before==snapshot(db) and len(client.calls)==2
    assert db.scalar(select(func.count()).select_from(ResearchSourceDocument))==1
    assert db.scalar(select(func.count()).select_from(ResearchEvidenceEvent))==0
    revision=db.scalar(select(DirectFeedRevision).join(DirectFeedDocument,DirectFeedRevision.document_id==DirectFeedDocument.id).where(DirectFeedDocument.source_key=='UL:reports:latest'))
    revision.source_bytes=b'tampered';db.commit()
    with pytest.raises(DirectSourceError,match='checksum'):issuer.prepared_transcript(db,'UL',observed_by=NOW)

def test_pending_rollover_and_failed_index_never_fall_back(db):
    client=Client(db);c=company()
    discovery.collect_discovery(db,client,[c],limit=2,recheck_hours=24)
    row=db.scalar(select(DirectFeedDocument).where(DirectFeedDocument.source_key=='UL:reports:latest'))
    row.checked_at=None;db.commit()
    client.raw=index(quarter=3,day='9 October 2026');client.body=transcript(quarter=3,day='9th October 2026')
    discovery.collect_discovery(db,client,[c],limit=1,recheck_hours=24)
    assert issuer.prepared_transcript(db,'UL',observed_by=NOW)['reason']=='reviewed_period_not_prepared'
    discovery.collect_discovery(db,client,[c],limit=1,recheck_hours=24)
    assert issuer.prepared_transcript(db,'UL',observed_by=NOW)['quarter']==3
    row.checked_at=None;db.commit();client.raw=b'<html>No current transcript</html>'
    result=discovery.collect_discovery(db,client,[c],limit=1,recheck_hours=24)
    assert result['errors']
    assert issuer.prepared_transcript(db,'UL',observed_by=NOW)['reason']=='latest_issuer_index_not_verified'

def test_stale_index_future_observation_and_removed_policy(db):
    client=Client(db);c=company()
    discovery.collect_discovery(db,client,[c],limit=2,recheck_hours=24)
    assert issuer.prepared_transcript(db,'UL',observed_by=NOW+timedelta(hours=49))['reason']=='latest_issuer_index_not_fresh'
    assert issuer.prepared_transcript(db,'UL',observed_by=NOW-timedelta(hours=1))['reason']=='latest_issuer_index_not_fresh'
    c.pop('discovery')
    assert discovery.collect_discovery(db,client,[c],limit=2,recheck_hours=24)['attempted']==0

def test_published_date_cannot_change_from_original(db):
    client=Client(db);c=company();discovery.collect_discovery(db,client,[c],limit=2,recheck_hours=24)
    m=policy.parse_index(client.raw,c,NOW)[0]
    m.update(published_at='2026-07-29',publication_pattern=policy.publication_pattern(date(2026,7,29)))
    with pytest.raises(DirectSourceError,match='publication date changed'):discovery.verified_metadata(db,c,m)

def test_attempt_budget_is_shared_with_existing_issuer(db):
    companies=[company(),deepcopy(next(c for c in json.loads(issuer.REGISTRY.read_bytes()) if c['symbol']=='MSFT'))]
    client=Client(db)
    result=discovery.collect_discovery(db,client,companies,limit=100,recheck_hours=1)
    assert result['attempted']==2 and len(client.calls)==2
    assert db.scalar(select(func.count()).select_from(DirectFeedDocument).where(DirectFeedDocument.source_key=='UL:earnings_transcript:2026:Q2'))==1


def test_benign_index_revision_retains_immutable_transcript_proof(db):
    client=Client(db);c=company()
    discovery.collect_discovery(db,client,[c],limit=2,recheck_hours=24)
    original=issuer.prepared_transcript(db,'UL',observed_by=NOW)
    row=db.scalar(select(DirectFeedDocument).where(DirectFeedDocument.source_key=='UL:reports:latest'))
    row.checked_at=None;db.commit()
    client.raw=client.raw.replace(b'</html>',b'<footer>Updated footer</footer></html>')
    result=discovery.collect_discovery(db,client,[c],limit=1,recheck_hours=24)
    assert result['processed']==1 and not result['errors']
    assert issuer.prepared_transcript(db,'UL',observed_by=NOW)==original
    assert db.scalar(select(func.count()).select_from(DirectFeedRevision))==3


def test_changed_call_date_same_url_is_held_until_reconciled(db):
    client=Client(db);c=company()
    discovery.collect_discovery(db,client,[c],limit=2,recheck_hours=24)
    row=db.scalar(select(DirectFeedDocument).where(DirectFeedDocument.source_key=='UL:reports:latest'))
    row.checked_at=None;db.commit();client.raw=index(day='29 July 2026')
    discovery.collect_discovery(db,client,[c],limit=1,recheck_hours=24)
    assert issuer.prepared_transcript(db,'UL',observed_by=NOW)['reason']=='reviewed_source_changed'
