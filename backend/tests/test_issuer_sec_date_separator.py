"""Complete current SEC sources and canonical consumer proof for dated headings."""
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
from app.services import issuer_sec_discovery as policy,direct_feed_store as store
from app.services.direct_feed_store import DirectFeedRevision,dumps
from app.services.research_evidence import upsert_source_document,EVIDENCE_PROCESSING_VERSION

ROOT=Path(__file__).parent/'fixtures/issuer_mga_nov1'
NOW=datetime(2026,11,1,23,24,33,tzinfo=timezone.utc)

def originals():
 manifest=json.loads((ROOT/'manifest.json').read_bytes());pages={}
 for url,row in manifest.items():
  raw=gzip.decompress((ROOT/(row['file']+'.gz')).read_bytes())
  assert len(raw)==row['bytes'] and hashlib.sha256(raw).hexdigest()==row['sha256']
  pages[url]=raw
 return pages

def company():return deepcopy(next(c for c in json.loads(issuer.REGISTRY.read_bytes()) if c['symbol']=='MGA'))

def candidate(pages):
 covers=policy.parse_index(pages[policy.INDEX_URL],company(),NOW)
 parent=next(m for m in covers if m['accession_number']=='0001104659-26-120638')
 metadata=policy.parse_event(pages[parent['url']],parent,NOW)[0]
 return metadata

@pytest.fixture(autouse=True)
def no_network(monkeypatch):
 def denied(*a,**kw):raise AssertionError('Unexpected external request')
 monkeypatch.setattr(socket.socket,'connect',denied)

def test_complete_current_heading_preserves_explicit_call_date_and_source():
 pages=originals();meta=candidate(pages)
 resolved,text,parsed=policy.parse_candidate(pages[meta['url']],meta,NOW)
 assert resolved==meta and 'transcript_date_source' not in resolved
 assert resolved['event_date']==resolved['published_at']=='2026-10-30'
 assert (resolved['fiscal_year'],resolved['fiscal_quarter'])==(2026,3)
 assert parsed['has_qa'] and len(text)==55940
 assert (resolved,text,parsed)==policy.parse_candidate(pages[meta['url']],meta,NOW)

@pytest.mark.parametrize('before,after',[
 (b'26OCT30',b'26OCT29'),
 (b'THIRD QUARTER 2026',b'SECOND QUARTER 2026'),
 (b'MAGNA INTERNATIONAL',b'OTHER COMPANY'),
 (b'WEBCAST 26OCT30',b'WEBCAST unverified date 26OCT30'),
 (b'WEBCAST 26OCT30',b'WEBCAST26OCT30'),
 (b'QUESTION &amp; ANSWER:',b'DISCUSSION:'),
 (b'first question comes from',b'opening remarks are from'),
 (b"This concludes today's call",b'This begins the presentation'),
])
def test_whole_original_still_requires_identity_date_adjacency_and_complete_qa(before,after):
 pages=originals();meta=candidate(pages);raw=pages[meta['url']]
 assert before in raw
 with pytest.raises(DirectSourceError):policy.parse_candidate(raw.replace(before,after),meta,NOW)

@pytest.mark.parametrize('historical',[False,True])
def test_complete_current_directory_collection_and_research_repeat(monkeypatch,historical):
 monkeypatch.setattr(discovery,'utcnow',lambda:NOW);monkeypatch.setattr(store,'utcnow',lambda:NOW)
 def clock(mapper,connection,target):target.fetched_at=NOW
 event.listen(DirectFeedRevision,'before_insert',clock)
 engine=create_engine('sqlite:///:memory:');Base.metadata.create_all(engine);pages=originals()
 def snapshot(db):return dumps([[t.name,sorted([dict(r._mapping) for r in db.execute(select(t))],key=dumps)] for t in Base.metadata.sorted_tables])
 try:
  with Session(engine) as db:
   class Client:
    issuer_document_urls=frozenset()
    def __init__(self):self.calls=[]
    def get(self,url):
     assert not db.in_transaction();assert url in pages
     assert url==policy.INDEX_URL or url in self.issuer_document_urls
     self.calls.append(url);return pages[url]
   client=Client()
   runs=[discovery.collect_discovery(db,client,[company()],limit=2,recheck_hours=24) for _ in range(2)]
   assert all(r['attempted']==r['processed']==2 and not r['errors'] for r in runs)
   assert len(client.calls)==4 and set(client.calls)==set(pages)
   prepared=issuer.prepared_transcript(db,'MGA',observed_by=NOW)
   assert prepared['status']=='prepared' and prepared['quarter']==3 and prepared['year']==2026
   assert prepared['published_date']=='2026-10-30' and len(prepared['content'])==55940
   security=Security(symbol='MGA',name='Magna International',asset_class='stock');db.add(security);db.commit()
   old=None
   if historical:
    old,_=upsert_source_document(db,security_id=security.id,document_type='earnings_transcript',source_provider='fmp',
     external_id=f'earnings_transcript:{security.id}:2026:Q3',content='Synthetic prior-provider rendering.',filing_type='Q3-2026',
     published_at=datetime(2026,10,30,tzinfo=timezone.utc))
    old.processing_status='processed';old.processing_version=EVIDENCE_PROCESSING_VERSION;db.commit()
    saved={col.name:getattr(old,col.name) for col in old.__table__.columns}
   boundary=date(2026,10,31) if historical else date(2026,10,1)
   first=issuer.prepare_research_document(db,security_id=security.id,publish_since=boundary);db.commit()
   assert first['status']==('retained_history' if historical else 'prepared')
   before=snapshot(db)
   again=issuer.prepare_research_document(db,security_id=security.id,publish_since=boundary);db.commit()
   assert first['document'].id==again['document'].id and snapshot(db)==before
   assert db.scalar(select(func.count()).select_from(ResearchSourceDocument))==1
   assert db.scalar(select(func.count()).select_from(ResearchEvidenceEvent))==0
   assert db.scalar(select(func.count()).select_from(EmailDelivery))==0
   if old is not None:assert {col.name:getattr(old,col.name) for col in old.__table__.columns}==saved
 finally:
  engine.dispose();event.remove(DirectFeedRevision,'before_insert',clock)
