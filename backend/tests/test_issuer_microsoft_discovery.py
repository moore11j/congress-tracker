"""Issuer rollover must preserve evidence, budgets and canonical research identity."""
from copy import deepcopy
from datetime import datetime, date, timezone, timedelta
import hashlib
import json
import socket

import pytest
from sqlalchemy import create_engine, select, func
from sqlalchemy.orm import Session

from app.db import Base
from app.models import Security, ResearchSourceDocument, ResearchEvidenceEvent
from app.clients.direct_sources import DirectSourceError
from app.services import issuer_discovery as discovery, issuer_transcripts as issuer
from app.services import issuer_microsoft_discovery as microsoft, direct_feed_store as store
from app.services.direct_feed_collection import collect_direct_feeds
from app.services.direct_feed_store import DirectFeedDocument, DirectFeedRevision

NOW = datetime(2026, 10, 15, tzinfo=timezone.utc)
URL = 'https://www.microsoft.com/en-us/investor/events/fy-2027/earnings-fy-2027-q1'


def company():
    return deepcopy(next(c for c in json.loads(issuer.REGISTRY.read_bytes()) if c['symbol'] == 'MSFT'))


def index(url=URL):
    return f'<main><h1>Microsoft Investor Relations</h1><a href="{url}">Webcast</a></main>'.encode()


def transcript(year=2027, quarter=1, stamp='October 9, 2026', qa=True):
    ordinal=microsoft.ORDINALS[quarter-1]
    text=f'Microsoft Fiscal Year {year} {ordinal} Quarter Earnings Conference Call Friday, {stamp} '
    text+=f'Transcript Microsoft FY{str(year)[2:]} {ordinal} Quarter Earnings Conference Call {stamp} '
    text+='Synthetic earnings management discussion. '*40
    if qa:text+='With that, let us go to Q&A. Analyst questions and management answers. '*10
    return ('<main>'+text+'</main>').encode()


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    calls=[]
    def denied(*args,**kwargs):
        calls.append(True)
        raise AssertionError('Network forbidden in issuer rehearsal')
    monkeypatch.setattr(socket.socket,'connect',denied)
    yield
    assert not calls


@pytest.fixture
def db(monkeypatch):
    monkeypatch.setattr(discovery,'utcnow',lambda:NOW)
    monkeypatch.setattr(store,'utcnow',lambda:NOW)
    engine=create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    with Session(engine) as session:yield session
    engine.dispose()


class Client:
    issuer_document_urls=frozenset()
    def __init__(self,db):self.db,self.calls=db,[]
    def get(self,url):
        assert not self.db.in_transaction()
        self.calls.append(url)
        if url==microsoft.INDEX_URL:return index()
        assert url==URL and url in self.issuer_document_urls
        return transcript()


@pytest.mark.parametrize('year,quarter,expected',[(2027,1,'2026-09-30'),(2027,2,'2026-12-31'),(2027,3,'2027-03-31'),(2026,4,'2026-06-30')])
def test_fiscal_dates_are_not_calendar_quarters(year,quarter,expected):
    assert microsoft.fiscal_end(year,quarter).isoformat()==expected


@pytest.mark.parametrize('url',[URL.replace('www.microsoft.com','evil.example'),URL+'?url=other',URL+'#fake',URL.replace('https:','http:'),URL.replace('www.microsoft.com','www.microsoft.com.evil.example'),URL.replace('/fy-2027/','/fy-2026/'),URL.replace('-q1','-q9'),URL.replace('/events/','/downloads/'),URL.replace('https://','https://user@')])
def test_index_never_grants_unreviewed_or_inconsistent_event_urls(url):
    with pytest.raises(DirectSourceError):microsoft.parse_index(index(url),company(),NOW)


def test_upcoming_announcement_does_not_discover_a_transcript():
    raw=b'<main>Upcoming Microsoft FY27 Q1 Earnings October 28, 2026 <a href="https://aka.ms/notify">Get Notification</a></main>'
    with pytest.raises(DirectSourceError):microsoft.parse_index(raw,company(),NOW)


def test_duplicate_navigation_links_have_one_period_and_immutable_index_hash():
    raw=index()+index()
    result=microsoft.parse_index(raw,company(),NOW)
    assert len(result)==1 and result[0]['source_page_sha256']==hashlib.sha256(raw).hexdigest()
    assert result[0]['period_end']=='2026-09-30' and 'published_at' not in result[0]


@pytest.mark.parametrize('change',['no_qa','future_date','wrong_year','wrong_quarter','wrong_company','invalid_date','early_date','far_date'])
def test_transcript_content_must_independently_prove_identity_and_qa(change):
    metadata=microsoft.parse_index(index(),company(),NOW)[0]
    raw=transcript()
    if change=='no_qa':raw=transcript(qa=False)
    if change=='future_date':raw=transcript(stamp='October 28, 2026')
    if change=='wrong_year':raw=transcript(year=2026)
    if change=='wrong_quarter':raw=transcript(quarter=2)
    if change=='wrong_company':raw=raw.replace(b'Microsoft',b'OtherCompany')
    if change=='invalid_date':raw=transcript(stamp='September 31, 2026')
    if change=='early_date':raw=transcript(stamp='September 29, 2026')
    if change=='far_date':raw=transcript(stamp='March 9, 2027')
    with pytest.raises(DirectSourceError):discovery.parse_candidate(raw,metadata,NOW)


def test_full_future_period_collection_reader_publication_and_repeat(db):
    c=company();client=Client(db)
    result=discovery.collect_discovery(db,client,[c],limit=2,recheck_hours=24)
    assert result=={'attempted':2,'processed':2,'errors':[],'stop_reason':None}
    ready=issuer.prepared_transcript(db,'MSFT',observed_by=NOW+timedelta(days=1))
    assert ready['status']=='prepared' and ready['year']==2027 and ready['quarter']==1
    assert ready['published_date']=='2026-10-09' and ready['has_qa']
    assert ready['source_page_sha256']==hashlib.sha256(index()).hexdigest()
    security=Security(symbol='MSFT',name='Microsoft',asset_class='stock');db.add(security);db.commit()
    first=issuer.prepare_research_document(db,security_id=security.id,publish_since=date(2026,10,1));db.commit()
    again=issuer.prepare_research_document(db,security_id=security.id,publish_since=date(2026,10,1));db.commit()
    assert first['created'] and not again['created'] and first['document'].id==again['document'].id
    assert first['publication_evidence']==again['publication_evidence']
    assert db.scalar(select(func.count()).select_from(ResearchSourceDocument))==1
    assert db.scalar(select(func.count()).select_from(ResearchEvidenceEvent))==0
    repeat=discovery.collect_discovery(db,client,[c],limit=2,recheck_hours=1)
    assert repeat['attempted']==0 and len(client.calls)==2
    assert db.scalar(select(func.count()).select_from(DirectFeedRevision))==2


def test_latest_pending_period_blocks_old_static_transcript(db):
    result=discovery.collect_discovery(db,Client(db),[company()],limit=1,recheck_hours=24)
    assert result['attempted']==1
    assert issuer.prepared_transcript(db,'MSFT')['reason']=='reviewed_period_not_prepared'


@pytest.mark.parametrize('mutation',['source_bytes','metadata_url','source_hash','publication_date'])
def test_reader_revalidates_original_index_and_document_proof(db,mutation):
    discovery.collect_discovery(db,Client(db),[company()],limit=2,recheck_hours=24)
    doc=db.scalar(select(DirectFeedDocument).where(DirectFeedDocument.feed=='issuer_earnings'))
    metadata=json.loads(doc.metadata_json)
    if mutation=='source_bytes':
        rev=db.scalar(select(DirectFeedRevision).join(DirectFeedDocument, DirectFeedRevision.document_id==DirectFeedDocument.id).where(DirectFeedDocument.feed==discovery.INDEX_FEED))
        rev.source_bytes=b'tampered'
    elif mutation=='metadata_url':metadata['url']=URL.replace('microsoft','other')
    elif mutation=='source_hash':metadata['source_page_sha256']='0'*64
    else:metadata['published_at']='2026-10-08'
    doc.metadata_json=store.dumps(metadata);db.commit()
    with pytest.raises((DirectSourceError,ValueError)):issuer.prepared_transcript(db,'MSFT')


def test_original_receipt_survives_index_revision(db,monkeypatch):
    client=Client(db);discovery.collect_discovery(db,client,[company()],limit=2,recheck_hours=24)
    before=issuer.prepared_transcript(db,'MSFT')
    monkeypatch.setattr(discovery,'utcnow',lambda:NOW+timedelta(days=1))
    monkeypatch.setattr(store,'utcnow',lambda:NOW+timedelta(days=1))
    class Changed(Client):
        def get(self,url):return index()+b'<!-- harmless revision -->' if url==microsoft.INDEX_URL else super().get(url)
    discovery.collect_discovery(db,Changed(db),[company()],limit=2,recheck_hours=24)
    after=issuer.prepared_transcript(db,'MSFT',observed_by=NOW+timedelta(days=1))
    assert after['source_page_sha256']==before['source_page_sha256']
    assert after['text_sha256']==before['text_sha256']


def test_existing_static_period_keeps_original_reviewed_metadata(db):
    c=company();current=issuer._reviewed_latest('MSFT')
    row=store.discover(db,'issuer_earnings',current);db.commit()
    class Existing(Client):
        def get(self,url):
            assert url==microsoft.INDEX_URL
            return index(current['url'])
    result=discovery.collect_discovery(db,Existing(db),[c],limit=1,recheck_hours=24)
    db.refresh(row)
    assert result['processed']==1 and json.loads(row.metadata_json)==current


def test_full_collector_never_adds_static_fetches_past_shared_budget(db,monkeypatch):
    monkeypatch.setenv('ISSUER_TRANSCRIPT_DISCOVERY_ENABLED','1')
    client=Client(db)
    result=collect_direct_feeds(db,client,sources=['issuer_earnings'],start=NOW.date(),end=NOW.date(),issuer_registry=[company()],limit=2)
    assert len(client.calls)==2 and result['issuer_discovery']['attempted']==1 and result['issuer_discovery']['reserved_static_requests']==1
    assert result['public_writes']==0


def test_interrupt_reserves_attempt_before_network(db):
    class Interrupted(BaseException):pass
    class Interrupt(Client):
        def get(self,url):raise Interrupted()
    with pytest.raises(Interrupted):discovery.collect_discovery(db,Interrupt(db),[company()],limit=2,recheck_hours=24)
    row=db.scalar(select(DirectFeedDocument))
    assert row.attempts==1 and row.checked_at is not None
    follow=Client(db)
    assert discovery.collect_discovery(db,follow,[company()],limit=2,recheck_hours=24)['attempted']==0
    assert not follow.calls


def test_microsoft_and_reddit_share_budget_and_both_make_progress(db):
    from test_issuer_discovery import company as reddit_company, raw_index, transcript as reddit_transcript
    class Mixed(Client):
        def get(self,url):
            assert not self.db.in_transaction()
            if 'redditinc.com/feed/' in url:
                self.calls.append(url)
                return b'{"GetFinancialReportListResult":[]}' if 'year=2025' in url else raw_index()
            if 's203.q4cdn.com/' in url:
                self.calls.append(url)
                assert url in self.issuer_document_urls
                return reddit_transcript()
            return super().get(url)
    client=Mixed(db)
    receipts=[discovery.collect_discovery(db,client,[company(),reddit_company()],limit=2,recheck_hours=24) for _ in range(3)]
    assert [r['attempted'] for r in receipts]==[2,2,1]
    assert not any(r['errors'] for r in receipts)
    assert issuer.prepared_transcript(db,'MSFT')['status']=='prepared'
    assert issuer.prepared_transcript(db,'RDDT')['status']=='prepared'
    assert discovery.collect_discovery(db,client,[company(),reddit_company()],limit=2,recheck_hours=24)['attempted']==0
    assert len(client.calls)==5


def test_removed_policy_cannot_fetch_discovered_pages(db):
    discovery.collect_discovery(db,Client(db),[company()],limit=1,recheck_hours=24)
    c=company();c.pop('discovery')
    client=Client(db)
    assert discovery.collect_discovery(db,client,[c],limit=2,recheck_hours=24)['attempted']==0
    assert not client.calls
