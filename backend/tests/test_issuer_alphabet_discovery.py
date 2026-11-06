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
from app.services import issuer_alphabet_discovery as policy,direct_feed_store as store
from app.services.direct_feed_store import DirectFeedRevision,DirectFeedDocument,dumps
from app.services.research_evidence import upsert_source_document,EVIDENCE_PROCESSING_VERSION

NOW=datetime(2026,11,6,22,50,0,tzinfo=timezone.utc)
ROOT=Path(__file__).parent/'fixtures/issuer_alphabet_nov6'
URL='https://abc.xyz/investor/events/event-details/2026/2026-Q3-Earnings-Call-2026-Ujou16CTw9/default.aspx'

def original(name):
 raw=gzip.decompress((ROOT/(name+'.gz')).read_bytes())
 proof=json.loads((ROOT/'manifest.json').read_bytes())[name]
 assert len(raw)==proof['bytes'] and hashlib.sha256(raw).hexdigest()==proof['sha256']
 return raw

def company():
 return deepcopy(next(c for c in json.loads(issuer.REGISTRY.read_bytes()) if c['symbol']=='GOOGL'))

@pytest.fixture(autouse=True)
def no_network(monkeypatch):
 def deny(*a,**kw):raise AssertionError('Unexpected external request')
 monkeypatch.setattr(socket.socket,'connect',deny)

def test_whole_current_index_and_pdf_prove_period_publication_and_qa():
 rows=policy.parse_index(original('reports-2026.json'),company(),2026,NOW)
 assert [(r['fiscal_year'],r['fiscal_quarter']) for r in rows]==[(2026,3),(2026,2),(2026,1)]
 assert rows[0]['url']==URL
 resolved,text,parsed=policy.parse_candidate(original('q3-2026.html'),rows[0],NOW)
 assert resolved['published_at']=='2026-10-28' and parsed['has_qa']
 assert len(text)==55041
 assert (resolved,text,parsed)==policy.parse_candidate(original('q3-2026.html'),rows[0],NOW)
 assert resolved['source_page_sha256']==hashlib.sha256(original('reports-2026.json')).hexdigest()

@pytest.mark.parametrize('mutation',['host','year','quarter','followup','query','fragment','credentials','traversal','encoding','type','category','language'])
def test_current_content_link_rejects_identity_and_delivery_changes(mutation):
 data=json.loads(original('reports-2026.json'))
 item=next(d for d in data['GetFinancialReportListResult'][0]['Documents'] if d['DocumentTitle']=='Webcast & Transcript')
 replacements={'host':('abc.xyz','example.com'),
  'year':('2026','2025'),'quarter':('Q3','Q2'),'followup':('Earnings','Follow-Up'),
  'credentials':('https://','https://user@'),'traversal':('/event-details/','/event-details/../'),
  'encoding':('Earnings','%45arnings')}
 if mutation in replacements:item['DocumentPath']=item['DocumentPath'].replace(*replacements[mutation])
 if mutation=='query':item['DocumentPath']+='?redirect=x'
 if mutation=='fragment':item['DocumentPath']+='#other'
 if mutation=='type':item['DocumentType']='File'
 if mutation=='category':item['DocumentCategory']='presentation'
 if mutation=='language':item['LanguageId']=2
 with pytest.raises(DirectSourceError):policy.parse_index(json.dumps(data).encode(),company(),2026,NOW)

@pytest.mark.parametrize('historical',[False,True])
def test_whole_source_collection_and_research_identity_repeat(monkeypatch,historical):
 monkeypatch.setattr(discovery,'utcnow',lambda:NOW);monkeypatch.setattr(store,'utcnow',lambda:NOW)
 def clock(mapper,connection,target):target.fetched_at=NOW
 event.listen(DirectFeedRevision,'before_insert',clock)
 engine=create_engine('sqlite:///:memory:');Base.metadata.create_all(engine)
 c=company();pages={policy.index_url(c,2026):original('reports-2026.json'),URL:original('q3-2026.html')}
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
   source=issuer.prepared_transcript(db,'GOOGL',observed_by=NOW)
   assert source['status']=='prepared' and source['year']==2026 and source['quarter']==3 and source['has_qa']
   assert source['published_date']=='2026-10-28'
   security=Security(symbol='GOOGL',name='Alphabet Inc.',asset_class='stock');db.add(security);db.commit()
   old=None
   if historical:
    old,_=upsert_source_document(db,security_id=security.id,document_type='earnings_transcript',source_provider='fmp',
     external_id=f'earnings_transcript:{security.id}:2026:Q3',content='Synthetic prior-provider rendering.',filing_type='Q3-2026',
     published_at=datetime(2026,10,28,tzinfo=timezone.utc))
    old.processing_status='processed';old.processing_version=EVIDENCE_PROCESSING_VERSION;db.commit()
    saved={col.name:getattr(old,col.name) for col in old.__table__.columns}
   boundary=date(2026,10,29) if historical else date(2026,10,1)
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

@pytest.mark.parametrize('quarter,published,sha',[
 (1,'2026-04-29','61b62a62aa6b31e3f6f75f870cef022cced1962f5109362d8331994160a905d3'),
 (2,'2026-07-22','6ae54d608efa57165694346203a958b588d287b0fb007ba3f29190e72b4b261f'),
 (3,'2026-10-28','b4bd90cdd1656a29991179e5cddb26309947d9c6975e943159736fc6d0dc4a2a')])
def test_complete_current_year_calls_and_unchanged_q2_text(quarter,published,sha):
 row=next(r for r in policy.parse_index(original('reports-2026.json'),company(),2026,NOW) if r['fiscal_quarter']==quarter)
 result=policy.parse_candidate(original(f'q{quarter}-2026.html'),row,NOW)
 assert result[0]['published_at']==published and result[2]['has_qa']
 assert hashlib.sha256(result[1].encode()).hexdigest()==sha
 assert result==policy.parse_candidate(original(f'q{quarter}-2026.html'),row,NOW)

@pytest.mark.parametrize('mutation',['missing_latest','conflicting_link','wrong_period','pagination','future','share_class'])
def test_complete_index_rejects_ambiguous_incomplete_or_wrong_identity(mutation):
 data=json.loads(original('reports-2026.json'));c=company();rows=data['GetFinancialReportListResult']
 item=next(d for d in rows[0]['Documents'] if d['DocumentTitle']=='Webcast & Transcript')
 if mutation=='missing_latest':item['DocumentTitle']='Webcast'
 if mutation=='conflicting_link':
  extra=deepcopy(item);extra['DocumentPath']=extra['DocumentPath'].replace('Ujou16CTw9','OtherToken');rows[0]['Documents'].append(extra)
 if mutation=='wrong_period':rows[0]['ReportDate']='06/30/2026 00:00:00'
 if mutation=='pagination':rows[:]=rows*3
 if mutation=='future':rows[0].update(ReportSubType='Fourth Quarter',ReportTitle='Fourth Quarter 2026',ReportDate='12/31/2026 00:00:00')
 if mutation=='share_class':c['symbol']='GOOG'
 with pytest.raises(DirectSourceError):policy.parse_index(json.dumps(data).encode(),c,2026,NOW)

def test_identical_split_quarter_link_is_coalesced():
 data=json.loads(original('reports-2026.json'));data['GetFinancialReportListResult'].append(deepcopy(data['GetFinancialReportListResult'][0]))
 assert len(policy.parse_index(json.dumps(data).encode(),company(),2026,NOW))==3

@pytest.mark.parametrize('mutation',['date','title','body_period','qa','quoted_body','duplicate_date'])
def test_call_page_requires_independent_heading_date_and_body(mutation):
 from lxml import html
 raw=original('q3-2026.html');tree=html.fromstring(raw)
 def node(token):return next(n for n in tree.xpath('//*[@class]') if token in n.get('class').split())
 title=node('evergreen-event-title');stamp=node('evergreen-event-date-time-text');body=node('evergreen-event-body')
 if mutation=='date':stamp.text='December 31, 2026 1:30 PM PT'
 if mutation=='title':title.text='2026 Q2 Earnings Call'
 if mutation in {'body_period','qa'}:
  text=body.text_content()
  if mutation=='body_period':text=text.replace('Third Quarter 2026','Second Quarter 2026')
  else:text=text.replace('first question comes','an introduction comes')
  body.clear();body.set('class','evergreen-event-body');body.text=text
 if mutation=='quoted_body':
  quote=html.Element('blockquote');body.getparent().replace(body,quote);quote.append(body)
 if mutation=='duplicate_date':stamp.getparent().append(deepcopy(stamp))
 row=policy.parse_index(original('reports-2026.json'),company(),2026,NOW)[0]
 with pytest.raises(DirectSourceError):policy.parse_candidate(html.tostring(tree),row,NOW)
