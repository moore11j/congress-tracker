"""Future-period discovery, hostile links, source proofs and shared request bounds."""
from copy import deepcopy
from datetime import datetime, timezone, date, timedelta
import json

import pytest
from sqlalchemy import create_engine, select, func
from sqlalchemy.orm import Session

from app.db import Base
from app.clients.direct_sources import DirectSourceError
from app.models import Security, ResearchSourceDocument, ResearchEvidenceEvent
from app.services import issuer_discovery as discovery, issuer_transcripts as issuer
from app.services.direct_feed_store import DirectFeedDocument, DirectFeedRevision, dumps
from app.services.direct_feed_collection import collect_direct_feeds
from test_issuer_pdf import pdf

NOW = datetime(2026, 10, 11, tzinfo=timezone.utc)


def company():
    return next(c for c in json.loads(issuer.REGISTRY.read_bytes()) if c['symbol'] == 'RDDT')


def index(quarter=3, year=2026):
    names = {v:k for k,v in discovery.QUARTERS.items()}
    end = date(year, quarter*3, discovery.calendar.monthrange(year, quarter*3)[1])
    return {'GetFinancialReportListResult': [{'ReportSubType': names[quarter], 'ReportYear': year,
        'ReportTitle': f'{names[quarter]} {year}', 'LanguageId': 1,
        'ReportDate': end.strftime('%m/%d/%Y 00:00:00'), 'Documents': [{
            'DocumentCategory': 'transcript', 'DocumentTitle': 'Earnings Call Transcript',
            'DocumentFileType': 'PDF', 'DocumentType': 'File', 'LanguageId': 1,
            'DocumentPath': f'https://s203.q4cdn.com/380862485/files/doc_financials/{year}/q{quarter}/Reddit-Q{quarter}-Call.pdf'}]}]}


def raw_index(**kwargs):
    return json.dumps(index(**kwargs)).encode()


def transcript(quarter=3, stamp='10 - 09 - 2026'):
    return pdf(f"TRANSCRIPT {stamp} Reddit, Inc. Reddit Q{quarter}'26 Earnings Call "
               'Operator: (Operator Instructions) Our first question comes from A Reviewer. '
               + 'Synthetic company earnings evidence and analyst responses. '*40)


@pytest.fixture
def setup(monkeypatch):
    monkeypatch.setattr(discovery, 'utcnow', lambda: NOW)
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    with Session(engine) as db:
        yield db
    engine.dispose()


class Client:
    issuer_document_urls = frozenset()

    def __init__(self, db):
        self.db, self.calls = db, []

    def get(self, url):
        assert not self.db.in_transaction(), 'network must not hold a database transaction'
        self.calls.append(url)
        if 'year=2025' in url:
            return b'{"GetFinancialReportListResult":[]}'
        if '/feed/' in url:
            return raw_index()
        assert url in self.issuer_document_urls
        return transcript()


@pytest.mark.parametrize('stage', ['index', 'pdf'])
def test_interrupted_discovery_reserves_daily_attempt_before_network(setup, stage):
    class Interrupted(BaseException): pass
    if stage == 'pdf':
        discovery.collect_discovery(setup,Client(setup),[company()],limit=2,recheck_hours=24)
    calls=[]
    class InterruptClient(Client):
        def get(self,url):
            calls.append(url)
            raise Interrupted()
    with pytest.raises(Interrupted):
        discovery.collect_discovery(setup,InterruptClient(setup),[company()],limit=1,recheck_hours=24)
    row=setup.scalar(select(DirectFeedDocument).where(DirectFeedDocument.source_url==calls[0]))
    assert row.attempts==1 and row.checked_at is not None and row.status=='pending'
    followup=Client(setup)
    discovery.collect_discovery(setup,followup,[company()],limit=2,recheck_hours=24)
    assert calls[0] not in followup.calls


def test_interrupted_static_page_reserves_daily_attempt_before_network(setup,monkeypatch):
    monkeypatch.delenv('ISSUER_TRANSCRIPT_DISCOVERY_ENABLED',raising=False)
    class Interrupted(BaseException):pass
    calls=[]
    class InterruptClient:
        def get(self,url):
            calls.append(url)
            raise Interrupted()
    args=dict(sources=['issuer_earnings'],start=date(2026,10,10),end=date(2026,10,10),issuer_registry=[company()],limit=2)
    with pytest.raises(Interrupted):collect_direct_feeds(setup,InterruptClient(),**args)
    result=collect_direct_feeds(setup,InterruptClient(),**args)
    assert len(calls)==1 and result['pending_documents']==1


def prepare(db):
    client = Client(db)
    first = discovery.collect_discovery(db, client, [company()], limit=2, recheck_hours=24)
    assert first['attempted'] == first['processed'] == 2 and not first['errors']
    second = discovery.collect_discovery(db, client, [company()], limit=2, recheck_hours=24)
    assert second['attempted'] == second['processed'] == 1 and not second['errors']
    return client


def test_new_quarter_is_discovered_prepared_and_reused_without_duplicate_research(setup):
    db = setup
    client = prepare(db)
    assert len(client.calls) == 3
    newest = issuer.prepared_transcript(db, 'RDDT')
    assert newest['status'] == 'prepared' and newest['quarter'] == 3 and newest['has_qa']
    assert newest['published_date'] == '2026-10-09'  # Never the September report end.
    security = Security(symbol='RDDT', name='Reddit', asset_class='stock')
    db.add(security); db.commit()
    first = issuer.prepare_research_document(db, security_id=security.id, publish_since=date(2026,10,1)); db.commit()
    second = issuer.prepare_research_document(db, security_id=security.id, publish_since=date(2026,10,1)); db.commit()
    assert first['created'] and not second['created'] and first['document'].id == second['document'].id
    assert db.scalar(select(func.count()).select_from(ResearchSourceDocument)) == 1
    assert db.scalar(select(func.count()).select_from(ResearchEvidenceEvent)) == 0
    repeat = discovery.collect_discovery(db, client, [company()], limit=2, recheck_hours=24)
    assert repeat['attempted'] == 0 and len(client.calls) == 3


def test_index_and_document_collection_share_the_existing_two_request_limit(setup, monkeypatch):
    monkeypatch.setenv('ISSUER_TRANSCRIPT_DISCOVERY_ENABLED', '1')
    client = Client(setup)
    result = collect_direct_feeds(setup, client, sources=['issuer_earnings'], start=date(2026,10,10),
        end=date(2026,10,10), issuer_registry=[company()], limit=2, recheck_hours=24)
    assert len(client.calls) == result['issuer_discovery']['attempted'] == 2
    assert result['public_writes'] == 0
    assert issuer.prepared_transcript(setup, 'RDDT')['reason'] == 'reviewed_period_not_prepared'


def test_index_cooldown_stops_remaining_issuer_requests(setup, monkeypatch):
    monkeypatch.setenv('ISSUER_TRANSCRIPT_DISCOVERY_ENABLED','1')
    calls=[]
    class Denied:
        issuer_document_urls=frozenset()
        def get(self,url):
            calls.append(url)
            raise DirectSourceError('Source HTTP 429: fixture')
    result=collect_direct_feeds(setup,Denied(),sources=['issuer_earnings'],start=date(2026,10,10),
        end=date(2026,10,10),issuer_registry=[company()],limit=2)
    assert len(calls)==1 and result['issuer_discovery']['stop_reason']=='Source HTTP 429: fixture'
    assert result['pending_documents']==2 and result['unresolved_documents']==3


@pytest.mark.parametrize('failure', [False, True])
def test_existing_reviewed_page_releases_transaction_and_counts_one_attempt(setup, monkeypatch, failure):
    monkeypatch.delenv('ISSUER_TRANSCRIPT_DISCOVERY_ENABLED', raising=False)
    class StaticClient:
        def get(self, url):
            assert not setup.in_transaction()
            if failure:
                raise DirectSourceError('Source HTTP 429: fixture')
            return transcript(quarter=2, stamp='07 - 30 - 2026').replace(b'never-matches',b'')
    # Static registry requires a named analyst. Keep its established validation.
    class GoodClient(StaticClient):
        def get(self, url):
            raw = super().get(url)
            if failure: return raw
            return pdf("TRANSCRIPT 07 - 30 - 2026 Reddit, Inc. Reddit Q2'26 Earnings Call "
                'Operator: (Operator Instructions) Our first question comes from Tom Champion from Piper Sandler. '
                + 'Synthetic earnings evidence. '*60)
    result = collect_direct_feeds(setup, GoodClient(), sources=['issuer_earnings'], start=date(2026,10,10),
        end=date(2026,10,10), issuer_registry=[company()], limit=2)
    row = setup.scalar(select(DirectFeedDocument).where(DirectFeedDocument.feed=='issuer_earnings'))
    assert row.attempts == 1 and row.status == ('failed' if failure else 'parsed')
    assert bool(result['errors']) == failure


@pytest.mark.parametrize('case', ['tenant','host','http','credentials','traversal','query','quarter_path',
    'release','wrong_year','quarter_end','future','duplicate','many_reports','many_documents','language'])
def test_index_does_not_grant_unreviewed_links_or_ambiguous_periods(case):
    payload = index()
    report = payload['GetFinancialReportListResult'][0]
    doc = report['Documents'][0]
    url = doc['DocumentPath']
    variants = {'tenant':url.replace('380862485','999'), 'host':url.replace('s203.q4cdn.com','evil.example'),
        'http':url.replace('https:','http:'), 'credentials':url.replace('https://','https://user@'),
        'traversal':url.replace('/q3/','/q3/../'), 'query':url+'?next=https://evil.example',
        'quarter_path':url.replace('/q3/','/q2/')}
    if case in variants: doc['DocumentPath'] = variants[case]
    if case == 'release': doc['DocumentTitle'] = 'Earnings Release'
    if case == 'wrong_year': report['ReportYear'] = 2025
    if case == 'quarter_end': report['ReportDate'] = '10/09/2026 00:00:00'
    if case == 'future': payload = index(quarter=4)
    if case == 'duplicate': payload['GetFinancialReportListResult'].append(deepcopy(report))
    if case == 'many_reports': payload['GetFinancialReportListResult'] *= 11
    if case == 'many_documents': report['Documents'] *= 31
    if case == 'language': doc['LanguageId'] = 2
    with pytest.raises(DirectSourceError):
        discovery.parse_index(json.dumps(payload).encode(), company(), 2026, NOW)


@pytest.mark.parametrize('case', ['future','before_period','invalid_date','wrong_company','wrong_quarter','missing_qa'])
def test_discovered_document_requires_its_own_company_date_period_and_qa(case):
    m = discovery.parse_index(raw_index(), company(), 2026, NOW)[0]
    stamps = {'future':'10 - 30 - 2026','before_period':'07 - 30 - 2026','invalid_date':'10 - 40 - 2026'}
    raw = transcript(quarter=2 if case == 'wrong_quarter' else 3, stamp=stamps.get(case, '10 - 09 - 2026'))
    if case == 'wrong_company': raw = pdf('TRANSCRIPT 10 - 09 - 2026 Other, Inc. '+('earnings transcript evidence '*70))
    if case == 'missing_qa': raw = pdf("TRANSCRIPT 10 - 09 - 2026 Reddit, Inc. Reddit Q3'26 Earnings Call "+('prepared remarks earnings '*70))
    with pytest.raises(DirectSourceError): discovery.parse_candidate(raw, m, NOW)


def test_reviewed_presentation_format_requires_speakers_participants_and_live_questions():
    m = discovery.parse_index(raw_index(quarter=1), company(), 2026, NOW)[0]
    text = "04 - 30 - 2026 Reddit Inc Reddit Q1'26 Earnings Call CORPORATE SPEAKERS: A Speaker "
    text += 'PARTICIPANTS: A Researcher PRESENTATION: Operator: Welcome to earnings. '
    text += 'Operator : First question comes from A Researcher. ' + 'Synthetic earnings evidence. '*50
    resolved, _, parsed = discovery.parse_candidate(pdf(text), m, NOW)
    assert resolved['published_at'] == '2026-04-30' and parsed['has_qa']
    assert resolved['transcript_publisher'] == 'Issuer-hosted transcript; publisher not identified'
    for omitted in ['CORPORATE SPEAKERS:', 'PARTICIPANTS:', 'PRESENTATION:', 'First question comes from']:
        with pytest.raises(DirectSourceError): discovery.parse_candidate(pdf(text.replace(omitted, '')), m, NOW)


@pytest.mark.parametrize('case', ['metadata','bytes','proof','url'])
def test_reader_revalidates_original_index_evidence(setup, case):
    prepare(setup)
    row = setup.scalar(select(DirectFeedDocument).where(DirectFeedDocument.feed=='issuer_earnings'))
    m = json.loads(row.metadata_json)
    if case == 'metadata': m['company_pattern']='.*'; row.metadata_json=dumps(m)
    if case == 'proof': m['source_page_sha256']='0'*64; row.metadata_json=dumps(m)
    if case == 'url': row.source_url = row.source_url.replace('380862485','999')
    if case == 'bytes':
        revision = setup.scalar(select(DirectFeedRevision).where(DirectFeedRevision.content_hash==m['source_page_sha256']))
        revision.source_bytes=b'{}'
    setup.commit()
    if case == 'url':
        assert issuer.prepared_transcript(setup,'RDDT')['reason']=='reviewed_source_changed'
    else:
        with pytest.raises(DirectSourceError): issuer.prepared_transcript(setup,'RDDT')


def test_changed_index_does_not_rewrite_first_proof_and_conflicting_url_holds(setup, monkeypatch):
    client = prepare(setup)
    row = setup.scalar(select(DirectFeedDocument).where(DirectFeedDocument.feed=='issuer_earnings'))
    first_metadata = row.metadata_json
    later = NOW+timedelta(days=1)
    monkeypatch.setattr(discovery, 'utcnow', lambda: later)
    payload = index(); payload['GetFinancialReportListResult'][0]['RevisionNumber'] = 10
    original_get = client.get
    client.get = lambda url: json.dumps(payload).encode() if 'year=2026' in url else original_get(url)
    discovery.collect_discovery(setup, client, [company()], limit=1, recheck_hours=24)
    setup.refresh(row)
    assert row.metadata_json == first_metadata
    # Force the index due again one day later; a changed document URL is held.
    monkeypatch.setattr(discovery, 'utcnow', lambda: later+timedelta(days=1))
    payload['GetFinancialReportListResult'][0]['Documents'][0]['DocumentPath'] = row.source_url.replace('Call.pdf','Revised.pdf')
    for index_row in setup.scalars(select(DirectFeedDocument).where(DirectFeedDocument.feed==discovery.INDEX_FEED)):
        index_row.checked_at = NOW-timedelta(days=1)
    setup.commit()
    result = discovery.collect_discovery(setup, client, [company()], limit=1, recheck_hours=24)
    setup.refresh(row)
    assert row.status == 'quarantined' and result['errors'] and row.metadata_json == first_metadata
    assert issuer.prepared_transcript(setup,'RDDT')['reason']=='reviewed_period_not_prepared'
