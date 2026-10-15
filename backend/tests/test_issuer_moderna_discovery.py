from copy import deepcopy
from datetime import date, datetime, timezone
import json
import socket
import pytest
from sqlalchemy import create_engine, select, func
from sqlalchemy.orm import Session
from app.db import Base
from app.models import Security, ResearchSourceDocument, ResearchEvidenceEvent
from app.clients.direct_sources import DirectSourceError
from app.services import issuer_discovery as discovery, issuer_transcripts as issuer, issuer_moderna_discovery as policy
from app.services import direct_feed_store as store
from app.services.direct_feed_store import DirectFeedDocument, DirectFeedRevision
from test_issuer_pdf import pdf

NOW=datetime(2026,10,15,tzinfo=timezone.utc)
def company():return deepcopy(next(c for c in json.loads(issuer.REGISTRY.read_bytes()) if c['symbol']=='MRNA'))
def event():return {'date':'October 9, 2026','event':'Moderna Third Quarter 2026 Conference Call','quarterlyResults':'Q3 2026','transcriptURL':'https://irp.cdn-website.com/89dd0539/files/uploaded/MRNA-USQ_Transcript_2026-10-09.pdf'}
def index(events=None):
    rows=events if events is not None else [event()]
    body=','.join('{'+','.join(k+':'+json.dumps(v) for k,v in r.items())+'}' for r in rows)
    return ('<html><script>const eventsMRNA=['+body+'];</script></html>').encode()
def transcript(heading=None,qa=True):
    return pdf((heading or 'LSEG STREETEVENTS EDITED TRANSCRIPT MRNA.OQ - Q3 2026 Moderna Inc Earnings Call EVENT DATE/TIME: OCTOBER 09, 2026 / 12:00PM GMT ')
               +'Synthetic earnings management discussion. '*45+('QUESTIONS AND ANSWERS Operator (Operator Instructions) Analyst. '*10 if qa else ''))

@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    calls=[]
    def deny(*a,**kw):calls.append(True);raise AssertionError('Unexpected network')
    monkeypatch.setattr(socket.socket,'connect',deny)
    yield
    assert not calls

@pytest.fixture
def db(monkeypatch):
    monkeypatch.setattr(discovery,'utcnow',lambda:NOW);monkeypatch.setattr(store,'utcnow',lambda:NOW)
    engine=create_engine('sqlite:///:memory:');Base.metadata.create_all(engine)
    with Session(engine) as session:yield session
    engine.dispose()

@pytest.mark.parametrize('script',[
 'const eventsMRNA=[{date:call()}]', 'const eventsMRNA=[{date:window.secret}]',
 'const eventsMRNA=[{date:"a",date:"b"}]', 'const eventsMRNA=[{date:42}]',
 'const eventsMRNA=[{date:{nested:"a"}}]', 'const eventsMRNA=[{date:"a"}];call()',
 'const eventsMRNA=[{date:"a"+"b"}]','const eventsMRNA=[{...spread}]',
 'const eventsMRNA=[{date:`template`}]','const eventsMRNA=[{date:"bad\\x31"}]',
 'const eventsMRNA=[', 'const eventsMRNA=[{date:"a"}',
])
def test_literal_data_never_executes_or_accepts_expressions(script):
    with pytest.raises(DirectSourceError):policy.literal_events(script)

def test_comments_strings_and_trailing_commas_remain_literal():
    assert policy.literal_events('const eventsMRNA=[// {bad:evil()}\n{url:"https://example.com/a//b",text:"/*data*/",},];')==[{'url':'https://example.com/a//b','text':'/*data*/'}]

@pytest.mark.parametrize('mutation',['host','tenant','query','fragment','http','credentials','date_file','title','date','future','quarter','duplicate'])
def test_bad_source_identity_is_held(mutation):
    row=event();rows=[row]
    if mutation=='host':row['transcriptURL']=row['transcriptURL'].replace('irp.cdn-website.com','evil.example')
    if mutation=='tenant':row['transcriptURL']=row['transcriptURL'].replace('89dd0539','foreign')
    if mutation in {'query','fragment'}:row['transcriptURL']+=('?' if mutation=='query' else '#')+'x'
    if mutation=='http':row['transcriptURL']=row['transcriptURL'].replace('https:','http:')
    if mutation=='credentials':row['transcriptURL']=row['transcriptURL'].replace('https://','https://user@')
    if mutation=='date_file':row['transcriptURL']=row['transcriptURL'].replace('10-09','10-08')
    if mutation=='title':row['event']='Another Company Third Quarter 2026 Conference Call'
    if mutation=='date':row['date']='October 99, 2026'
    if mutation=='future':row['date']='November 6, 2026'
    if mutation=='quarter':row['quarterlyResults']='Q4 2026'
    if mutation=='duplicate':rows.append(deepcopy(row))
    with pytest.raises(DirectSourceError):policy.parse_index(index(rows),company(),NOW)

@pytest.mark.parametrize('mutation',['publisher','company','quarter','date','qa'])
def test_pdf_must_independently_match_index_and_have_qa(mutation):
    metadata=policy.parse_index(index(),company(),NOW)[0]
    heading='LSEG STREETEVENTS EDITED TRANSCRIPT MRNA.OQ - Q3 2026 Moderna Inc Earnings Call EVENT DATE/TIME: OCTOBER 09, 2026 / 12:00PM GMT '
    if mutation=='publisher':heading=heading.replace('LSEG','REFINITIV')
    if mutation=='company':heading=heading.replace('Moderna','Another')
    if mutation=='quarter':heading=heading.replace('Q3','Q2')
    if mutation=='date':heading=heading.replace('09,','08,')
    with pytest.raises(DirectSourceError):policy.parse_candidate(transcript(heading,mutation!='qa'),metadata,NOW)

def test_unpublished_upcoming_and_unrelated_events_do_not_grant_urls():
    upcoming={'date':'November 6, 2026','event':'Moderna Third Quarter 2026 Conference Call','quarterlyResults':'','transcriptURL':''}
    assert len(policy.parse_index(index([upcoming,{'quarterlyResults':'','transcriptURL':'https://evil.example/a.pdf'},event()]),company(),NOW))==1

@pytest.mark.parametrize('raw',[b'<html/>',b'<script>const eventsMRNA=[];</script>',index()+index()])
def test_missing_or_ambiguous_index_is_not_success(raw):
    with pytest.raises(DirectSourceError):policy.parse_index(raw,company(),NOW)

def test_publisher_uses_call_year_and_exact_reviewed_tenant():
    row=event();row.update(date='February 13, 2026',event='Moderna Fourth Quarter 2025 Conference Call',quarterlyResults='Q4 2025',transcriptURL='https://irp.cdn-website.com/0a71bb4f/files/uploaded/MRNA-USQ_Transcript_2026-02-13.pdf')
    m=policy.parse_index(index([row]),company(),NOW)[0]
    assert m['transcript_publisher']=='LSEG StreetEvents' and m['fiscal_year']==2025
    row.update(date='November 6, 2025',event='Moderna Third Quarter 2025 Conference Call',quarterlyResults='Q3 2025',transcriptURL='https://irp.cdn-website.com/89dd0539/files/uploaded/MRNA-USQ_Transcript_2025-11-06+%281%29.pdf')
    assert policy.parse_index(index([row]),company(),NOW)[0]['transcript_publisher']=='Refinitiv StreetEvents'

class Client:
    issuer_document_urls=frozenset()
    def __init__(self,db):self.db,self.calls=db,[]
    def get(self,url):
        assert not self.db.in_transaction();self.calls.append(url)
        if url==policy.INDEX_URL:return index()
        assert url==event()['transcriptURL'] and url in self.issuer_document_urls
        return transcript()

def test_collector_reader_repeat_and_tamper(db):
    client=Client(db);c=company()
    result=discovery.collect_discovery(db,client,[c],limit=20,recheck_hours=1)
    assert result['attempted']==result['processed']==2 and not result['errors']
    source=issuer.prepared_transcript(db,'MRNA');assert source['quarter']==3 and source['has_qa']
    security=Security(symbol='MRNA',name='Synthetic Moderna',asset_class='stock');db.add(security);db.commit()
    assert issuer.prepare_research_document(db,security_id=security.id,publish_since=date(2026,10,1))['created'];db.commit()
    before={t.name:[dict(r._mapping) for r in db.execute(select(t))] for t in Base.metadata.sorted_tables}
    assert not issuer.prepare_research_document(db,security_id=security.id,publish_since=date(2026,10,1))['created'];db.commit()
    assert discovery.collect_discovery(db,client,[c],limit=2,recheck_hours=1)['attempted']==0
    assert before=={t.name:[dict(r._mapping) for r in db.execute(select(t))] for t in Base.metadata.sorted_tables}
    assert len(client.calls)==2 and db.scalar(select(func.count()).select_from(ResearchSourceDocument))==1
    assert db.scalar(select(func.count()).select_from(ResearchEvidenceEvent))==0
    revision=db.scalar(select(DirectFeedRevision).join(DirectFeedDocument,DirectFeedRevision.document_id==DirectFeedDocument.id).where(DirectFeedDocument.source_key=='MRNA:reports:latest'))
    revision.source_bytes=b'tampered';db.commit()
    with pytest.raises(DirectSourceError,match='checksum'):issuer.prepared_transcript(db,'MRNA')

def test_pending_period_and_removed_policy(db):
    client=Client(db);c=company()
    discovery.collect_discovery(db,client,[c],limit=1,recheck_hours=24)
    assert issuer.prepared_transcript(db,'MRNA')['reason']=='reviewed_period_not_prepared'
    c.pop('discovery')
    assert discovery.collect_discovery(db,client,[c],limit=2,recheck_hours=24)['attempted']==0


def test_original_index_publication_date_cannot_be_rewritten(db):
    client=Client(db)
    discovery.collect_discovery(db,client,[company()],limit=2,recheck_hours=24)
    row=db.scalar(select(DirectFeedDocument).where(DirectFeedDocument.source_key=='MRNA:earnings_transcript:2026:Q3'))
    metadata=json.loads(row.metadata_json)
    metadata.update(published_at='2026-10-10',publication_pattern=policy.publication_pattern(date(2026,10,10)))
    with pytest.raises(DirectSourceError,match='publication date changed'):
        discovery.verified_metadata(db,company(),metadata)


def test_current_static_period_keeps_reviewed_proof(db):
    c=company();static=issuer._reviewed_latest('MRNA')
    row=store.discover(db,'issuer_earnings',static);db.commit()
    record=event();record.update(date='July 31, 2026',event='Moderna Second Quarter 2026 Conference Call',quarterlyResults='Q2 2026',transcriptURL=static['url'])
    class Existing(Client):
        def get(self,url):return index([record])
    discovery.collect_discovery(db,Existing(db),[c],limit=1,recheck_hours=24);db.refresh(row)
    assert row.source_url==static['url'] and json.loads(row.metadata_json)==static


@pytest.mark.parametrize('script', ['const eventsMRNA=['+','.join('{}' for _ in range(301))+']', 'const eventsMRNA=[{'+','.join('k'+str(i)+':"x"' for i in range(31))+'}]', 'const eventsMRNA=[];'+(' '*1_000_001)])
def test_literal_resource_bounds(script):
    with pytest.raises(DirectSourceError):policy.literal_events(script)
