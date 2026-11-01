"""Complete issuer originals, strict URL identity and no-duplicate consumer replay."""
from copy import deepcopy
from datetime import datetime,date,timezone
from pathlib import Path
import gzip,hashlib,json,socket

import pytest
from sqlalchemy import create_engine,select,func,event
from sqlalchemy.orm import Session
from app.db import Base
from app.models import Security,ResearchSourceDocument,ResearchEvidenceEvent,EmailDelivery
from app.clients.direct_sources import DirectSourceError
from app.services import issuer_discovery as discovery,issuer_transcripts as issuer
from app.services import issuer_q4_pdf_discovery as policy,direct_feed_store as store
from app.services.direct_feed_store import DirectFeedRevision,DirectFeedDocument,dumps
from app.services.research_evidence import upsert_source_document,EVIDENCE_PROCESSING_VERSION

NOW=datetime(2026,11,1,23,12,37,tzinfo=timezone.utc)
ROOT=Path(__file__).parent/'fixtures/issuer_sofi_nov1'
URL='https://s27.q4cdn.com/749715820/files/content_files/SoFi-Third-Quarter-2026-Earnings-Transcript_FINAL-docx.pdf'

def original(name):
 raw=gzip.decompress((ROOT/(name+'.gz')).read_bytes())
 proof=json.loads((ROOT/'manifest.json').read_bytes())[name]
 assert len(raw)==proof['bytes'] and hashlib.sha256(raw).hexdigest()==proof['sha256']
 return raw

def company():
 return deepcopy(next(c for c in json.loads(issuer.REGISTRY.read_bytes()) if c['symbol']=='SOFI'))

@pytest.fixture(autouse=True)
def no_network(monkeypatch):
 def deny(*a,**kw):raise AssertionError('Unexpected external request')
 monkeypatch.setattr(socket.socket,'connect',deny)

def test_whole_current_index_and_pdf_prove_period_publication_and_qa():
 rows=policy.parse_index(original('index.json'),company(),2026,NOW)
 assert [(r['fiscal_year'],r['fiscal_quarter']) for r in rows]==[(2026,3),(2026,2),(2026,1)]
 assert rows[0]['url']==URL
 resolved,text,parsed=policy.parse_candidate(original('sofi-q3-2026.pdf'),rows[0],NOW)
 assert resolved['published_at']=='2026-10-27' and parsed['has_qa']
 assert len(text)==75727
 assert (resolved,text,parsed)==policy.parse_candidate(original('sofi-q3-2026.pdf'),rows[0],NOW)
 assert resolved['source_page_sha256']==hashlib.sha256(original('index.json')).hexdigest()

@pytest.mark.parametrize('mutation',['tenant','host','year','quarter','followup','query','fragment','credentials','traversal','encoding','type','category','language'])
def test_current_content_link_rejects_identity_and_delivery_changes(mutation):
 data=json.loads(original('index.json'))
 item=next(d for d in data['GetFinancialReportListResult'][0]['Documents'] if d['DocumentTitle']=='Transcript')
 replacements={'tenant':('749715820','999999999'),'host':('s27.q4cdn.com','example.com'),
  'year':('2026','2025'),'quarter':('Third','Second'),'followup':('Earnings','Follow-Up'),
  'credentials':('https://','https://user@'),'traversal':('/content_files/','/content_files/../'),
  'encoding':('Third','%54hird')}
 if mutation in replacements:item['DocumentPath']=item['DocumentPath'].replace(*replacements[mutation])
 if mutation=='query':item['DocumentPath']+='?redirect=x'
 if mutation=='fragment':item['DocumentPath']+='#other'
 if mutation=='type':item['DocumentType']='Link'
 if mutation=='category':item['DocumentCategory']='presentation'
 if mutation=='language':item['LanguageId']=2
 with pytest.raises(DirectSourceError):policy.parse_index(json.dumps(data).encode(),company(),2026,NOW)

@pytest.mark.parametrize('historical',[False,True])
def test_whole_source_collection_and_research_identity_repeat(monkeypatch,historical):
 monkeypatch.setattr(discovery,'utcnow',lambda:NOW);monkeypatch.setattr(store,'utcnow',lambda:NOW)
 def clock(mapper,connection,target):target.fetched_at=NOW
 event.listen(DirectFeedRevision,'before_insert',clock)
 engine=create_engine('sqlite:///:memory:');Base.metadata.create_all(engine)
 c=company();pages={policy.index_url(c,2026):original('index.json'),URL:original('sofi-q3-2026.pdf')}
 def snapshot(db):
  return dumps([[t.name,sorted([dict(r._mapping) for r in db.execute(select(t))],key=dumps)] for t in Base.metadata.sorted_tables])
 try:
  with Session(engine) as db:
   # A prior-year fetch is outside this bounded current-period replay. Its
   # explicitly failed receipt prevents inventing successful past coverage.
   prior=store.discover(db,discovery.INDEX_FEED,discovery.index_metadata(c,2025))
   prior.status='failed';prior.checked_at=NOW;db.commit()
   class Client:
    issuer_document_urls=frozenset()
    def __init__(self):self.calls=[]
    def get(self,url):
     assert not db.in_transaction();assert url in pages
     if url==URL:assert url in self.issuer_document_urls
     self.calls.append(url);return pages[url]
   client=Client()
   runs=[discovery.collect_discovery(db,client,[c],limit=1,recheck_hours=24) for _ in range(2)]
   assert all(r['attempted']==r['processed']==1 and not r['errors'] for r in runs)
   assert client.calls==list(pages)
   source=issuer.prepared_transcript(db,'SOFI',observed_by=NOW)
   assert source['status']=='prepared' and source['year']==2026 and source['quarter']==3 and source['has_qa']
   assert source['published_date']=='2026-10-27'
   security=Security(symbol='SOFI',name='SoFi Technologies',asset_class='stock');db.add(security);db.commit()
   old=None
   if historical:
    old,_=upsert_source_document(db,security_id=security.id,document_type='earnings_transcript',source_provider='fmp',
     external_id=f'earnings_transcript:{security.id}:2026:Q3',content='Synthetic prior-provider rendering.',filing_type='Q3-2026',
     published_at=datetime(2026,10,27,tzinfo=timezone.utc))
    old.processing_status='processed';old.processing_version=EVIDENCE_PROCESSING_VERSION;db.commit()
    saved={col.name:getattr(old,col.name) for col in old.__table__.columns}
   boundary=date(2026,10,28) if historical else date(2026,10,1)
   first=issuer.prepare_research_document(db,security_id=security.id,publish_since=boundary);db.commit()
   assert first['status']==('retained_history' if historical else 'prepared')
   full=snapshot(db)
   second=issuer.prepare_research_document(db,security_id=security.id,publish_since=boundary);db.commit()
   assert snapshot(db)==full and first['document'].id==second['document'].id
   assert db.scalar(select(func.count()).select_from(ResearchSourceDocument))==1
   assert db.scalar(select(func.count()).select_from(ResearchEvidenceEvent))==0
   assert db.scalar(select(func.count()).select_from(EmailDelivery))==0
   if old is not None:assert {col.name:getattr(old,col.name) for col in old.__table__.columns}==saved
 finally:
  engine.dispose();event.remove(DirectFeedRevision,'before_insert',clock)
