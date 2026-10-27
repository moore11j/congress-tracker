"""Free issuer PDF discovery: fiscal identity, exact links, shared budget and proofs."""
from copy import deepcopy
from datetime import date, datetime, timezone, timedelta
import json
import re
import socket

import pytest
from issuer_fixture_clock import source_clock
from sqlalchemy import create_engine, select, func
from sqlalchemy.orm import Session

from app.db import Base
from app.models import Security, ResearchSourceDocument, ResearchEvidenceEvent
from app.clients.direct_sources import DirectSourceError
from app.services import issuer_discovery as discovery, issuer_transcripts as issuer
from app.services import issuer_q4_pdf_discovery as policy, direct_feed_store as store
from app.services.direct_feed_collection import collect_direct_feeds
from app.services.direct_feed_store import DirectFeedDocument, DirectFeedRevision
from test_issuer_pdf import pdf

NOW = datetime(2026, 10, 15, tzinfo=timezone.utc)


def company(symbol):
    return deepcopy(next(c for c in json.loads(issuer.REGISTRY.read_bytes()) if c['symbol']==symbol))


def index(symbol,quarter=3,year=2026):
    spec=policy.POLICIES[symbol];label=policy.QUARTERS[quarter-1]
    end=date(year,quarter*3,policy.calendar.monthrange(year,quarter*3)[1])
    item={'DocumentCategory':spec['category'],'DocumentTitle':spec['title'],
        'DocumentPath':spec['cdn']+f'{year}/q{quarter}/{symbol}-Earnings-Transcript.pdf',
        'DocumentFileType':'PDF','DocumentType':'File','LanguageId':1}
    return {'GetFinancialReportListResult':[{'ReportSubType':label,'ReportYear':year,'ReportTitle':f'{label} {year}',
        'LanguageId':1,'ReportDate':end.strftime('%m/%d/%Y 00:00:00'),'Documents':[item]}]}


def raw_index(symbol,**kwargs):return json.dumps(index(symbol,**kwargs)).encode()


def transcript(symbol,quarter=3,year=2026,stamp=None,qa=True):
    label=policy.QUARTERS[quarter-1]
    if symbol=='META':
        heading=f'1 Meta Platforms, Inc. (META) {label} {year} Results Conference Call {stamp or "October 9th, 2026"} '
        questions='And your first question comes from the line of An Analyst. '
    else:
        heading=f'TRANSCRIPT {stamp or "10 - 09 - 2026"} SoFi Technologies SOFI Q{quarter} {year} Earnings Conference Call '
        questions='Operator^ (Operator Instructions) Our first question today comes from An Analyst. '
    return pdf(heading+'Synthetic earnings discussion with company management. '*45+(questions*15 if qa else ''))


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    calls=[]
    def denied(*args,**kwargs):calls.append(True);raise AssertionError('Network forbidden')
    monkeypatch.setattr(socket.socket,'connect',denied)
    yield
    assert not calls


@pytest.fixture
def db(monkeypatch):
    monkeypatch.setattr(discovery,'utcnow',lambda:NOW)
    monkeypatch.setattr(store,'utcnow',lambda:NOW)
    engine=create_engine('sqlite:///:memory:');Base.metadata.create_all(engine)
    with Session(engine) as db:yield db
    engine.dispose()


class Client:
    issuer_document_urls=frozenset()
    def __init__(self,db):self.db,self.calls=db,[]
    def get(self,url):
        assert not self.db.in_transaction()
        self.calls.append(url)
        for symbol in policy.KINDS:
            if url==policy.index_url(company(symbol),2026):return raw_index(symbol)
            if url==policy.index_url(company(symbol),2025):return b'{"GetFinancialReportListResult":[]}'
            if url==index(symbol)['GetFinancialReportListResult'][0]['Documents'][0]['DocumentPath']:
                assert url in self.issuer_document_urls
                return transcript(symbol)
        raise AssertionError('Unexpected URL')


@pytest.mark.parametrize('symbol',['META','SOFI'])
@pytest.mark.parametrize('mutation',['foreign_host','sibling_tenant','wrong_year','wrong_quarter','http','credentials','query','fragment','followup_label','category','filetype','language','duplicate'])
def test_index_rejects_unreviewed_links_and_conflicting_documents(symbol,mutation):
    data=index(symbol);docs=data['GetFinancialReportListResult'][0]['Documents'];item=docs[0]
    if mutation=='foreign_host':item['DocumentPath']='https://example.com/a.pdf'
    if mutation=='sibling_tenant':item['DocumentPath']=item['DocumentPath'].replace('/files/','/other/files/')
    if mutation=='wrong_year':item['DocumentPath']=item['DocumentPath'].replace('/2026/','/2025/')
    if mutation=='wrong_quarter':item['DocumentPath']=item['DocumentPath'].replace('/q3/','/q2/')
    if mutation=='http':item['DocumentPath']=item['DocumentPath'].replace('https:','http:')
    if mutation=='credentials':item['DocumentPath']=item['DocumentPath'].replace('https://','https://user@')
    if mutation=='query':item['DocumentPath']+='?redirect=elsewhere'
    if mutation=='fragment':item['DocumentPath']+='#other'
    if mutation=='category':item['DocumentCategory']='unreviewed'
    if mutation=='filetype':item['DocumentFileType']='HTML'
    if mutation=='language':item['LanguageId']=2
    if mutation=='duplicate':docs.append(deepcopy(item))
    if mutation=='followup_label':
        item['DocumentTitle']='Follow Up Call Transcript'
        assert policy.parse_index(json.dumps(data).encode(),company(symbol),2026,NOW)==[]
        return
    with pytest.raises(DirectSourceError):policy.parse_index(json.dumps(data).encode(),company(symbol),2026,NOW)


def test_meta_followup_call_does_not_replace_earnings_call():
    data=index('META');docs=data['GetFinancialReportListResult'][0]['Documents']
    followup={**docs[0],'DocumentTitle':'Follow Up Call Transcript','DocumentPath':'https://example.com/followup.pdf'}
    docs.insert(0,followup)
    result=policy.parse_index(json.dumps(data).encode(),company('META'),2026,NOW)
    assert len(result)==1 and result[0]['url']==docs[1]['DocumentPath']


@pytest.mark.parametrize('symbol',['META','SOFI'])
@pytest.mark.parametrize('mutation',['year','title','end_date','language','duplicate_period','bad_row','bad_docs','size','future_period'])
def test_index_report_identity_and_bounds(symbol,mutation):
    data=index(symbol);report=data['GetFinancialReportListResult'][0]
    if mutation=='year':report['ReportYear']=2025
    if mutation=='title':report['ReportTitle']='Other Report'
    if mutation=='end_date':report['ReportDate']='09/29/2026 00:00:00'
    if mutation=='language':report['LanguageId']=2
    if mutation=='duplicate_period':data['GetFinancialReportListResult'].append(deepcopy(report))
    if mutation=='bad_row':data['GetFinancialReportListResult']=[None]
    if mutation=='bad_docs':report['Documents']=[None]
    if mutation=='future_period':data=index(symbol,quarter=4)
    raw=b'x'*2_000_001 if mutation=='size' else json.dumps(data).encode()
    with pytest.raises(DirectSourceError):policy.parse_index(raw,company(symbol),2026,NOW)


@pytest.mark.parametrize('symbol',['META','SOFI'])
@pytest.mark.parametrize('mutation',['company','period','missing_qa','future_date','early_date','invalid_date','heading','followup'])
def test_pdf_independently_proves_call_identity_and_date(symbol,mutation):
    metadata=policy.parse_index(raw_index(symbol),company(symbol),2026,NOW)[0]
    raw=transcript(symbol)
    if mutation=='company':raw=transcript('SOFI' if symbol=='META' else 'META')
    if mutation=='period':raw=transcript(symbol,quarter=2)
    if mutation=='missing_qa':raw=transcript(symbol,qa=False)
    if mutation=='future_date':raw=transcript(symbol,stamp='October 28th, 2026' if symbol=='META' else '10 - 28 - 2026')
    if mutation=='early_date':raw=transcript(symbol,stamp='September 29th, 2026' if symbol=='META' else '09 - 29 - 2026')
    if mutation=='invalid_date':raw=transcript(symbol,stamp='September 31st, 2026' if symbol=='META' else '09 - 31 - 2026')
    if mutation=='heading':raw=pdf('Different heading. '+('Meta Platforms, Inc. Third Quarter 2026 Results Conference Call October 9th, 2026 Your first question comes from X. ' if symbol=='META' else 'SoFi Technologies Q3 2026 Earnings TRANSCRIPT 10 - 09 - 2026 Operator^ Our first question comes from X. ')*30)
    if mutation=='followup':raw=pdf(('Meta Platforms, Inc. (META) Third Quarter 2026 Follow Up Conference Call October 9th, 2026 Your first question comes from X. ' if symbol=='META' else 'SoFi Technologies Q3 2026 Earnings Follow Up TRANSCRIPT 10 - 09 - 2026 Operator^ Our first question comes from X. ')*30)
    with pytest.raises(DirectSourceError):policy.parse_candidate(raw,metadata,NOW)


@pytest.mark.parametrize('symbol',['META','SOFI'])
def test_fourth_quarter_publication_uses_next_calendar_year(symbol):
    metadata=policy.parse_index(raw_index(symbol,quarter=4,year=2025),company(symbol),2025,NOW)[0]
    stamp='January 30th, 2026' if symbol=='META' else '1 - 30 - 2026'
    resolved,text,parsed=policy.parse_candidate(transcript(symbol,quarter=4,year=2025,stamp=stamp),metadata,NOW)
    assert resolved['published_at']=='2026-01-30' and resolved['fiscal_year']==2025
    assert parsed['has_qa'] and re.search(resolved['publication_pattern'],text)


def test_numeric_date_pattern_preserves_reviewed_unicode_separator():
    assert re.search(policy.publication_pattern('SOFI',date(2026,1,30)),'1 \u2013 30 - 2026')
    assert re.search(policy.publication_pattern('SOFI',date(2026,7,29)),'07 \u2013 29 - 2026')


def test_both_issuers_share_two_fetch_budget_and_repeat_without_duplicates(db):
    client=Client(db);companies=[company(s) for s in policy.KINDS]
    runs=[discovery.collect_discovery(db,client,companies,limit=2,recheck_hours=24) for _ in range(3)]
    assert [r['attempted'] for r in runs]==[2,2,2] and not any(r['errors'] for r in runs)
    for symbol in policy.KINDS:
        source=issuer.prepared_transcript(db,symbol)
        assert source['status']=='prepared' and source['quarter']==3 and source['published_date']=='2026-10-09'
        assert source['transcript_publisher']=='Issuer-hosted transcript; publisher not identified'
        security=Security(symbol=symbol,name='Synthetic '+symbol,asset_class='stock');db.add(security);db.commit()
        first=issuer.prepare_research_document(db,security_id=security.id,publish_since=date(2026,10,1));db.commit()
        repeat=issuer.prepare_research_document(db,security_id=security.id,publish_since=date(2026,10,1));db.commit()
        assert first['created'] and not repeat['created'] and first['publication_evidence']==repeat['publication_evidence']
    assert db.scalar(select(func.count()).select_from(ResearchSourceDocument))==2
    assert db.scalar(select(func.count()).select_from(ResearchEvidenceEvent))==0
    assert discovery.collect_discovery(db,client,companies,limit=2,recheck_hours=1)['attempted']==0
    assert len(client.calls)==6


@pytest.mark.parametrize('symbol',['META','SOFI'])
def test_static_current_period_keeps_its_original_url_and_proof(db,symbol):
    c=company(symbol);metadata=issuer._reviewed_latest(symbol)
    row=store.discover(db,'issuer_earnings',metadata);db.commit()
    class Existing(Client):
        def get(self,url):return raw_index(symbol,quarter=2)
    discovery.collect_discovery(db,Existing(db),[c],limit=1,recheck_hours=24)
    db.refresh(row)
    assert row.source_url==metadata['url'] and json.loads(row.metadata_json)==metadata


@pytest.mark.parametrize('symbol',['META','SOFI'])
def test_pending_new_period_prevents_fallback_and_policy_removal_prevents_fetch(db,symbol):
    client=Client(db);c=company(symbol)
    discovery.collect_discovery(db,client,[c],limit=1,recheck_hours=24)
    assert issuer.prepared_transcript(db,symbol)['reason']=='reviewed_period_not_prepared'
    c.pop('discovery')
    assert discovery.collect_discovery(db,client,[c],limit=2,recheck_hours=24)['attempted']==0
    assert len(client.calls)==1


@pytest.mark.parametrize('symbol',['META','SOFI'])
def test_index_original_tampering_is_rejected(db,symbol):
    client=Client(db)
    for _ in range(2):discovery.collect_discovery(db,client,[company(symbol)],limit=2,recheck_hours=24)
    revision=db.scalar(select(DirectFeedRevision).join(DirectFeedDocument,DirectFeedRevision.document_id==DirectFeedDocument.id).where(DirectFeedDocument.source_key==symbol+':reports:2026'))
    revision.source_bytes=b'tampered';db.commit()
    with pytest.raises(DirectSourceError,match='checksum'):issuer.prepared_transcript(db,symbol)


def test_main_collector_cannot_add_static_fetches_after_discovery_budget(db,monkeypatch):
    monkeypatch.setenv('ISSUER_TRANSCRIPT_DISCOVERY_ENABLED','1')
    client=Client(db)
    result=collect_direct_feeds(db,client,sources=['issuer_earnings'],start=NOW.date(),end=NOW.date(),issuer_registry=[company('META'),company('SOFI')],limit=2)
    assert len(client.calls)==2 and result['issuer_discovery']['attempted']==1 and result['issuer_discovery']['reserved_static_requests']==1 and result['public_writes']==0


@pytest.mark.parametrize('intro',['Our first question','Your first question','First question today','And our first question today'])
def test_reviewed_operator_variants_keep_real_qa_requirement(intro):
    metadata=policy.parse_index(raw_index('SOFI'),company('SOFI'),2026,NOW)[0]
    content=('TRANSCRIPT 10 - 09 - 2026 SoFi Technologies Q3 2026 Earnings Conference Call '
             +'Synthetic earnings discussion. '*50+'Operator^ (Operator Instructions) '+intro+' comes from An Analyst. ')
    resolved,text,parsed=policy.parse_candidate(pdf(content),metadata,NOW)
    assert parsed['has_qa'] and resolved['published_at']=='2026-10-09'

def test_captured_sofi_ligature_operator_line_matches_without_text_rewriting():
    line='Operator^ (Operator Instructions) Your \ufb01rst question comes from Devin Ryan with Citizens Bank.'
    assert re.search(policy.POLICIES['SOFI']['qa_pattern'],line,re.I)

@pytest.mark.parametrize('variant', ['valid', 'host', 'tenant', 'year', 'quarter', 'followup', 'query', 'fragment', 'credentials', 'type'])
def test_meta_current_content_directory_keeps_exact_call_identity(variant):
    data=index('META');item=data['GetFinancialReportListResult'][0]['Documents'][0]
    item['DocumentPath']='https://s21.q4cdn.com/399680738/files/content_files/META-Q3-2026-Earnings-Call-Transcript.pdf'
    item['DocumentType']='file'
    if variant=='host':item['DocumentPath']=item['DocumentPath'].replace('s21.q4cdn.com','example.com')
    if variant=='tenant':item['DocumentPath']=item['DocumentPath'].replace('399680738','999999999')
    if variant=='year':item['DocumentPath']=item['DocumentPath'].replace('2026','2025')
    if variant=='quarter':item['DocumentPath']=item['DocumentPath'].replace('Q3','Q2')
    if variant=='followup':item['DocumentPath']=item['DocumentPath'].replace('Earnings','Follow-Up')
    if variant=='query':item['DocumentPath']+='?x=1'
    if variant=='fragment':item['DocumentPath']+='#x'
    if variant=='credentials':item['DocumentPath']=item['DocumentPath'].replace('https://','https://user@')
    if variant=='type':item['DocumentType']='Link'
    if variant=='valid':
        result=policy.parse_index(json.dumps(data).encode(),company('META'),2026,NOW)
        assert len(result)==1 and result[0]['url']==item['DocumentPath']
    else:
        with pytest.raises(DirectSourceError):policy.parse_index(json.dumps(data).encode(),company('META'),2026,NOW)
