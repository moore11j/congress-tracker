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
from app.services import issuer_discovery as discovery, issuer_transcripts as issuer, issuer_ibm_discovery as policy
from app.services import direct_feed_store as store
from app.services.direct_feed_store import DirectFeedDocument, DirectFeedRevision
from test_issuer_pdf import pdf

NOW = datetime(2026, 10, 15, tzinfo=timezone.utc)
EVENT = 'https://www.ibm.com/investor/events/earnings-3q26'
PDF = 'https://www.ibm.com/downloads/documents/us-en/0123456789abcdef'

def company():
    return deepcopy(next(c for c in json.loads(issuer.REGISTRY.read_bytes()) if c['symbol'] == 'IBM'))

def index():
    return ('<html><aside>2026 Q4 <a href="https://evil.example">Earnings announcement</a></aside>'
            '<c4d-cta-block-item>2026 Q3 <a href="'+EVENT+'">Earnings announcement</a></c4d-cta-block-item></html>').encode()

def event():
    return ('<html><h1>IBM 3Q 2026 Earnings Announcement</h1><p>October 9, 2026, 5:00PM ET</p>'
            '<a href="https://evil.example/remarks">Prepared remarks</a><a href="'+PDF+'">Webcast transcript</a></html>').encode()

def transcript(text=None):
    return pdf(text or ('IBM 3Q26 Earnings Transcript 1 IBM 3Q 2026 Earnings Conference Call Transcript '
        'IBM Participants: Example Executive October 9, 2026 / 5:00 PM ET '
        +'Synthetic management discussion. '*50
        +'QUESTION AND ANSWER SECTION Operator: Our first question comes from Analyst. '+ 'Synthetic answer. '*30))

@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    calls = []
    def deny(*a, **kw):
        calls.append(True)
        raise AssertionError('Unexpected network')
    monkeypatch.setattr(socket.socket, 'connect', deny)
    yield
    assert not calls

@pytest.fixture
def db(monkeypatch):
    monkeypatch.setattr(discovery, 'utcnow', lambda: NOW)
    monkeypatch.setattr(store, 'utcnow', lambda: NOW)
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        yield session
    engine.dispose()

class Client:
    issuer_document_urls = frozenset()
    def __init__(self, db):
        self.db, self.calls = db, []
        self.pages = {policy.INDEX_URL: index(), EVENT: event(), PDF: transcript()}
    def get(self, url):
        assert not self.db.in_transaction()
        assert url == policy.INDEX_URL or url in self.issuer_document_urls
        self.calls.append(url)
        return self.pages[url]

def collect(db):
    client = Client(db)
    first = discovery.collect_discovery(db, client, [company()], limit=99, recheck_hours=1)
    assert first['attempted'] == first['processed'] == 2 and not first['errors']
    assert issuer.prepared_transcript(db, 'IBM', observed_by=NOW)['reason'] == 'reviewed_period_not_prepared'
    last = discovery.collect_discovery(db, client, [company()], limit=99, recheck_hours=1)
    assert last['attempted'] == last['processed'] == 1 and not last['errors']
    return client

def test_chain_budget_reader_and_whole_database_repeat(db, monkeypatch):
    client = collect(db)
    source = issuer.prepared_transcript(db, 'IBM')
    assert source['quarter'] == 3 and source['has_qa'] and source['source_url'] == PDF
    security = Security(symbol='IBM', name='Synthetic IBM', asset_class='stock')
    db.add(security); db.commit()
    assert issuer.prepare_research_document(db, security_id=security.id, publish_since=date(2026, 10, 1))['created']
    db.commit()
    before = {t.name: [dict(r._mapping) for r in db.execute(select(t))] for t in Base.metadata.sorted_tables}
    assert not issuer.prepare_research_document(db, security_id=security.id, publish_since=date(2026, 10, 1))['created']
    db.commit()
    assert discovery.collect_discovery(db, client, [company()], limit=2, recheck_hours=1)['attempted'] == 0
    assert before == {t.name: [dict(r._mapping) for r in db.execute(select(t))] for t in Base.metadata.sorted_tables}
    assert client.calls == [policy.INDEX_URL, EVENT, PDF]
    assert db.scalar(select(func.count()).select_from(ResearchSourceDocument)) == 1
    assert db.scalar(select(func.count()).select_from(ResearchEvidenceEvent)) == 0

@pytest.mark.parametrize('feed', [discovery.INDEX_FEED, policy.EVENT_FEED])
def test_each_immutable_hop_is_required(db, feed):
    collect(db)
    revision = db.scalar(select(DirectFeedRevision).join(DirectFeedDocument, DirectFeedRevision.document_id == DirectFeedDocument.id).where(DirectFeedDocument.feed == feed))
    revision.source_bytes = b'tampered'; db.commit()
    with pytest.raises(DirectSourceError, match='checksum'):
        issuer.prepared_transcript(db, 'IBM', observed_by=NOW)

@pytest.mark.parametrize('mutation', ['date', 'pdf', 'event', 'parent_key', 'recursive', 'stage', 'publisher'])
def test_stored_metadata_cannot_grant_different_sources(db, mutation):
    collect(db)
    row = db.scalar(select(DirectFeedDocument).where(DirectFeedDocument.feed == 'issuer_earnings'))
    m = json.loads(row.metadata_json)
    if mutation == 'date': m['published_at'] = '2026-10-23'
    if mutation == 'pdf': m['url'] = PDF[:-1]+'0'
    if mutation == 'event': m['source_page_url'] = EVENT+'-other'
    if mutation == 'parent_key': m['parent_event']['key'] = 'IBM:earnings_transcript:2026:Q2'
    if mutation == 'recursive': m['parent_event']['parent_event'] = {}
    if mutation == 'stage': m['parent_event']['discovery_stage'] = 'pdf'
    if mutation == 'publisher': m['transcript_publisher'] = 'Other'
    with pytest.raises(DirectSourceError): discovery.verified_metadata(db, company(), m)

@pytest.mark.parametrize('mutation', ['remarks', 'host', 'query', 'ambiguous', 'company', 'quarter', 'date', 'future'])
def test_event_cannot_substitute_remarks_or_unreviewed_sources(mutation):
    raw = event()
    if mutation == 'remarks': raw = raw.replace(b'Webcast transcript', b'Prepared remarks')
    if mutation == 'host': raw = raw.replace(PDF.encode(), b'https://evil.example/transcript')
    if mutation == 'query': raw = raw.replace(PDF.encode(), (PDF+'?x=1').encode())
    if mutation == 'ambiguous': raw += ('<a href="'+PDF[:-1]+'0">Webcast transcript</a>').encode()
    if mutation == 'company': raw = raw.replace(b'IBM 3Q', b'Other 3Q')
    if mutation == 'quarter': raw = raw.replace(b'IBM 3Q', b'IBM 2Q')
    if mutation == 'date': raw = raw.replace(b'October 9', b'October 99')
    if mutation == 'future': raw = raw.replace(b'October 9', b'October 29')
    with pytest.raises(DirectSourceError): policy.parse_event(raw, policy.parse_index(index(), company(), NOW)[0], NOW)

@pytest.mark.parametrize('mutation', ['company', 'quarter', 'date', 'qa'])
def test_pdf_has_independent_fiscal_date_and_actual_qa(mutation):
    m = policy.parse_event(event(), policy.parse_index(index(), company(), NOW)[0], NOW)
    _, text, _ = policy.parse_candidate(transcript(), m, NOW)
    if mutation == 'company': text = text.replace('IBM', 'Other')
    if mutation == 'quarter': text = text.replace('3Q', '2Q')
    if mutation == 'date': text = text.replace('October 9', 'October 21')
    if mutation == 'qa': text = text.replace('Our first question', 'Prepared statements')
    with pytest.raises(DirectSourceError): policy.parse_candidate(transcript(text), m, NOW)

def test_upcoming_banner_does_not_admit_a_period():
    rows = policy.parse_index(index(), company(), NOW)
    assert len(rows) == 1 and rows[0]['fiscal_quarter'] == 3

def test_missing_transcript_holds_new_period_and_obeys_daily_retry(db):
    client = Client(db); client.pages[EVENT] = event().replace(b'Webcast transcript', b'Prepared remarks')
    result = discovery.collect_discovery(db, client, [company()], limit=2, recheck_hours=1)
    assert result['processed'] == 1 and len(result['errors']) == 1
    assert issuer.prepared_transcript(db, 'IBM', observed_by=NOW)['reason'] == 'reviewed_period_not_prepared'
    assert discovery.collect_discovery(db, client, [company()], limit=2, recheck_hours=1)['attempted'] == 0
    assert client.calls == [policy.INDEX_URL, EVENT]

def test_static_current_quarter_keeps_original_reviewed_proof(db):
    c = company(); static = issuer._reviewed_latest('IBM')
    row = store.discover(db, 'issuer_earnings', static); db.commit()
    client = Client(db); client.pages[policy.INDEX_URL] = index().replace(b'2026 Q3', b'2026 Q2').replace(b'3q26', b'2q26')
    result = discovery.collect_discovery(db, client, [c], limit=2, recheck_hours=24)
    assert result['attempted'] == 1 and not result['errors']
    db.refresh(row)
    assert row.source_url == static['url'] and json.loads(row.metadata_json) == static
    assert db.scalar(select(func.count()).select_from(DirectFeedDocument).where(DirectFeedDocument.feed == policy.EVENT_FEED)) == 0

def test_removed_policy_stops_pending_hops(db):
    client = Client(db); c = company()
    discovery.collect_discovery(db, client, [c], limit=1, recheck_hours=24)
    c.pop('discovery')
    assert discovery.collect_discovery(db, client, [c], limit=2, recheck_hours=24)['attempted'] == 0


@pytest.mark.parametrize('mutation', ['missing', 'duplicate', 'future', 'wrong_link', 'oversized'])
def test_index_rejects_unreviewed_or_ambiguous_periods(mutation):
    raw = index()
    if mutation == 'missing': raw = b'<html/>'
    if mutation == 'duplicate': raw += index()
    if mutation == 'future': raw = raw.replace(b'2026 Q3', b'2026 Q4').replace(b'3q26', b'4q26')
    if mutation == 'wrong_link': raw = raw.replace(EVENT.encode(), b'https://evil.example')
    if mutation == 'oversized': raw += b' ' * 2_000_001
    with pytest.raises(DirectSourceError): policy.parse_index(raw, company(), NOW)


def test_later_index_and_event_revisions_preserve_original_chain(db):
    collect(db)
    source = issuer.prepared_transcript(db, 'IBM')
    for feed in (discovery.INDEX_FEED, policy.EVENT_FEED):
        row = db.scalar(select(DirectFeedDocument).where(DirectFeedDocument.feed == feed))
        store.record_document(db, row, b'<html>New reviewed content</html>', 'New reviewed content', {})
        db.commit()
    assert issuer.prepared_transcript(db, 'IBM') == source


def test_event_source_change_quarantines_pdf_without_overwriting_proof(db, monkeypatch):
    client = collect(db)
    row = db.scalar(select(DirectFeedDocument).where(DirectFeedDocument.feed == 'issuer_earnings'))
    old_metadata = row.metadata_json
    event_row = db.scalar(select(DirectFeedDocument).where(DirectFeedDocument.feed == policy.EVENT_FEED))
    event_row.checked_at = None; db.commit()
    client.pages[EVENT] = event().replace(PDF.encode(), (PDF[:-1]+'0').encode())
    result = discovery.collect_discovery(db, client, [company()], limit=2, recheck_hours=24)
    assert len(result['errors']) == 1
    db.refresh(row)
    assert row.status == 'quarantined' and row.metadata_json == old_metadata and row.source_url == PDF
    assert issuer.prepared_transcript(db, 'IBM')['reason'] == 'reviewed_period_not_prepared'



@pytest.mark.parametrize('state', ['pending', 'failed', 'quarantined'])
def test_outer_report_keeps_unresolved_event_hops_partial(db, monkeypatch, state):
    from app.services.direct_feed_collection import collect_direct_feeds
    monkeypatch.setenv('ISSUER_TRANSCRIPT_DISCOVERY_ENABLED', '1')
    client = Client(db); c = company(); c['documents'] = []
    first = collect_direct_feeds(db, client, sources=['issuer_earnings'], start=NOW.date(),
        end=NOW.date(), issuer_registry=[c], limit=1, recheck_hours=24)
    assert first['pending_documents'] == first['unresolved_documents'] == 1
    assert first['status'] == 'partial' and len(client.calls) == 1
    event_row = db.scalar(select(DirectFeedDocument).where(DirectFeedDocument.feed == policy.EVENT_FEED))
    event_row.status = state; event_row.checked_at = NOW
    event_row.error = ('Issuer period changed source URL; review required' if state == 'quarantined' else None)
    db.commit()
    repeat = collect_direct_feeds(db, client, sources=['issuer_earnings'], start=NOW.date(),
        end=NOW.date(), issuer_registry=[c], limit=2, recheck_hours=24)
    assert repeat['status'] == 'partial' and repeat['unresolved_documents'] == 1
    assert repeat['pending_documents'] == int(state == 'pending')
    assert not repeat['errors'] and len(client.calls) == 1 and repeat['public_writes'] == 0

