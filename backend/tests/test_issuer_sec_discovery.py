from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
import json
import socket

import pytest
from sqlalchemy import create_engine, select, func, event as sql_event
from sqlalchemy.orm import Session
from app.db import Base
from app.models import Security, ResearchSourceDocument, ResearchEvidenceEvent
from app.clients.direct_sources import DirectSourceError
from app.services import issuer_discovery as discovery, issuer_transcripts as issuer, issuer_sec_discovery as policy
from app.services import direct_feed_store as store
from app.services.direct_feed_store import DirectFeedDocument, DirectFeedRevision

NOW = datetime(2026, 10, 15, 18, tzinfo=timezone.utc)
ACCESSION = '0001104659-26-089231'
COVER = 'https://www.sec.gov/Archives/edgar/data/749098/000110465926089231/tm2618646d2_6k.htm'
TRANSCRIPT = COVER.replace('_6k.htm', '_ex99-3.htm')


def company():
    return deepcopy(next(c for c in json.loads(issuer.REGISTRY.read_bytes()) if c['symbol'] == 'MGA'))


def index(extra=()):
    entries = [(ACCESSION, '6-K', '2026-07-31', 'tm2618646d2_6k.htm'), *extra]
    return json.dumps({'cik': 749098, 'name': 'MAGNA INTERNATIONAL INC', 'tickers': ['MGA'],
        'filings': {'recent': {k: [e[i] for e in entries] for i, k in enumerate(
            ['accessionNumber', 'form', 'filingDate', 'primaryDocument'])}}}).encode()


def cover(label='Q2 2026 Results Webcast Transcript (July 31, 2026)', href='tm2618646d2_ex99-3.htm', day='July 31, 2026'):
    return ('<html><p>FORM 6-K MAGNA INTERNATIONAL INC. Commission File Number 001-11444</p> '
            '<p>Date: '+day+'</p> <a href="'+href+'">'+label+'</a></html>').encode()


def transcript():
    return ('<DOCUMENT><TYPE>EX-99.3 <SEQUENCE>4 <FILENAME>tm2618646d2_ex99-3.htm <DESCRIPTION>EXHIBIT 99.3 <TEXT>'
        '<html><p>Exhibit 99.3 MAGNA INTERNATIONAL - SECOND QUARTER 2026 RESULTS WEBCAST &ndash; 26JUL31</p> '
        '<p>DISCLAIMER: This transcript is derived from a recording of the webcast. The transcription may still contain inaccuracies, errors, or omissions.</p> '
        '<p>CORPORATE SPEAKERS: Executive PARTICIPANTS: Analyst PRESENTATION: Earnings discussion. '
        +'Synthetic source material. '*50+'</p> <p>QUESTION &amp; ANSWER: Operator^ (Operator Instructions) And your first question comes from Analyst. '
        +'Synthetic response. '*30+"</p> <p>Operator^ Ladies and gentlemen, this does conclude today's conference call. Thank you.</p> </html></TEXT></DOCUMENT>").encode()


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def deny(*args, **kwargs):
        raise AssertionError('Unexpected network')
    monkeypatch.setattr(socket.socket, 'connect', deny)


@pytest.fixture
def db(monkeypatch):
    monkeypatch.setattr(discovery, 'utcnow', lambda: NOW)
    monkeypatch.setattr(store, 'utcnow', lambda: NOW)
    monkeypatch.setattr(DirectFeedRevision.__table__.c.fetched_at.default, 'arg', lambda ctx: store.utcnow())
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None): return store.utcnow()
    monkeypatch.setattr(issuer, 'datetime', Clock)
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        yield session
    engine.dispose()


class Client:
    issuer_document_urls = frozenset()
    def __init__(self, db):
        self.db, self.calls = db, []
        self.pages = {policy.INDEX_URL: index(), COVER: cover(), TRANSCRIPT: transcript()}
    def get(self, url):
        assert not self.db.in_transaction()
        assert url == policy.INDEX_URL or url in self.issuer_document_urls
        self.calls.append(url)
        return self.pages[url]


def collect(db, client=None):
    client = client or Client(db)
    first = discovery.collect_discovery(db, client, [company()], limit=99, recheck_hours=1)
    assert first['attempted'] == first['processed'] == 2 and not first['errors']
    assert issuer.prepared_transcript(db, 'MGA', observed_by=NOW)['reason'] == 'reviewed_period_not_prepared'
    last = discovery.collect_discovery(db, client, [company()], limit=99, recheck_hours=1)
    assert last['attempted'] == last['processed'] == 1 and not last['errors']
    return client


def test_complete_chain_same_contract_repeat_and_no_events(db):
    client = collect(db)
    source = issuer.prepared_transcript(db, 'MGA', observed_by=NOW)
    assert source['status'] == 'prepared' and source['has_qa'] and source['quarter'] == 2
    assert source['published_date'] == '2026-07-31' and source['source_url'] == TRANSCRIPT
    sec = Security(symbol='MGA', name='Synthetic Magna', asset_class='stock')
    db.add(sec); db.commit()
    result = issuer.prepare_research_document(db, security_id=sec.id, publish_since=date(2026, 7, 1))
    assert result['created']; db.commit()
    before = {t.name: [dict(r._mapping) for r in db.execute(select(t))] for t in Base.metadata.sorted_tables}
    assert not issuer.prepare_research_document(db, security_id=sec.id, publish_since=date(2026, 7, 1))['created']
    db.commit()
    assert discovery.collect_discovery(db, client, [company()], limit=2, recheck_hours=1)['attempted'] == 0
    assert before == {t.name: [dict(r._mapping) for r in db.execute(select(t))] for t in Base.metadata.sorted_tables}
    assert client.calls == [policy.INDEX_URL, COVER, TRANSCRIPT]
    assert db.scalar(select(func.count()).select_from(ResearchEvidenceEvent)) == 0
    assert db.scalar(select(func.count()).select_from(ResearchSourceDocument)) == 1


@pytest.mark.parametrize('feed', [discovery.INDEX_FEED, discovery.EVENT_FEED, 'issuer_earnings'])
def test_each_original_checksum_required(db, feed):
    collect(db)
    rev = db.scalar(select(DirectFeedRevision).join(DirectFeedDocument, DirectFeedDocument.id == DirectFeedRevision.document_id).where(DirectFeedDocument.feed == feed))
    rev.source_bytes = b'tampered'; db.commit()
    with pytest.raises((DirectSourceError, ValueError), match='checksum'):
        issuer.prepared_transcript(db, 'MGA', observed_by=NOW)


@pytest.mark.parametrize('mutation', ['company', 'cik', 'ticker', 'columns', 'duplicate', 'path', 'future', 'bound'])
def test_directory_rejects_unverified_identity_and_coverage(mutation):
    data = json.loads(index()); recent = data['filings']['recent']
    if mutation == 'company': data['name'] = 'Other'
    if mutation == 'cik': data['cik'] = 1
    if mutation == 'ticker': data['tickers'] = ['OTHER']
    if mutation == 'columns': recent['form'] = []
    if mutation == 'duplicate':
        for a in recent.values(): a.extend(a[:])
    if mutation == 'path': recent['primaryDocument'][0] = '../other.htm'
    if mutation == 'future': recent['filingDate'][0] = '2026-10-16'
    if mutation == 'bound':
        data = json.loads(index([(f'0001104659-26-{i:06d}', '6-K', '2026-01-01', 'cover.htm') for i in range(65)]))
    with pytest.raises(DirectSourceError): policy.parse_index(json.dumps(data).encode(), company(), NOW)


@pytest.mark.parametrize('mutation', ['issuer', 'commission', 'date', 'call_date', 'host', 'query', 'period', 'ambiguous'])
def test_cover_rejects_ambiguous_dates_periods_and_links(mutation):
    raw = cover()
    if mutation == 'issuer': raw = raw.replace(b'MAGNA', b'OTHER')
    if mutation == 'commission': raw = raw.replace(b'001-11444', b'001-99999')
    if mutation == 'date': raw = raw.replace(b'Date: July 31', b'Date: July 30')
    if mutation == 'call_date': raw = raw.replace(b'(July 31', b'(July 99')
    if mutation == 'host': raw = raw.replace(b'tm2618646d2_ex99-3.htm', b'https://evil.example/exhibit.htm')
    if mutation == 'query': raw = raw.replace(b'tm2618646d2_ex99-3.htm', b'tm2618646d2_ex99-3.htm?foo=1')
    if mutation == 'period': raw = raw.replace(b'Q2 2026', b'Q3 2026')
    if mutation == 'ambiguous': raw += b'<a href="other.htm">Transcript</a>'
    with pytest.raises(DirectSourceError): policy.parse_event(raw, policy.parse_index(index(), company(), NOW)[0], NOW)


@pytest.mark.parametrize('mutation', ['company', 'quarter', 'stamp', 'envelope', 'qa', 'closing', 'disclaimer'])
def test_transcript_has_independent_identity_full_qa_and_ending(mutation):
    raw = transcript()
    if mutation == 'company': raw = raw.replace(b'MAGNA', b'OTHER')
    if mutation == 'quarter': raw = raw.replace(b'SECOND QUARTER', b'FIRST QUARTER')
    if mutation == 'stamp': raw = raw.replace(b'26JUL31', b'26JUL30')
    if mutation == 'envelope': raw = raw.replace(b'tm2618646d2_ex99-3.htm', b'other.htm')
    if mutation == 'qa': raw = raw.replace(b'first question comes from', b'prepared remarks by')
    if mutation == 'closing': raw = raw.replace(b'does conclude', b'may start')
    if mutation == 'disclaimer': raw = raw.replace(b'inaccuracies, errors, or omissions', b'other things')
    m = policy.parse_event(cover(), policy.parse_index(index(), company(), NOW)[0], NOW)[0]
    with pytest.raises(DirectSourceError): policy.parse_candidate(raw, m, NOW)


def test_call_and_filing_dates_remain_distinct():
    raw = index().replace(b'2026-07-31', b'2026-08-03')
    m = policy.parse_event(cover(day='August 3, 2026'), policy.parse_index(raw, company(), NOW)[0], NOW)[0]
    assert m['published_at'] == '2026-08-03' and m['event_date'] == '2026-07-31'
    assert m['period_end'] == '2026-06-30'
    policy.parse_candidate(transcript(), m, NOW)


def test_same_day_directory_original_can_mature_without_changing_hash(db, monkeypatch):
    first = datetime(2026, 7, 31, 18, tzinfo=timezone.utc)
    monkeypatch.setattr(discovery, 'utcnow', lambda: first)
    monkeypatch.setattr(store, 'utcnow', lambda: first)
    client = Client(db)
    result = discovery.collect_discovery(db, client, [company()], limit=2, recheck_hours=24)
    assert result['processed'] == 1 and len(result['errors']) == 1
    assert issuer.prepared_transcript(db, 'MGA', observed_by=first)['status'] == 'unavailable'
    second = first + timedelta(days=1)
    monkeypatch.setattr(discovery, 'utcnow', lambda: second)
    monkeypatch.setattr(store, 'utcnow', lambda: second)
    for _ in range(2):
        assert not discovery.collect_discovery(db, client, [company()], limit=2, recheck_hours=24)['errors']
    assert issuer.prepared_transcript(db, 'MGA', observed_by=second)['status'] == 'prepared'
    assert db.scalar(select(func.count()).select_from(DirectFeedRevision)) == 3


@pytest.mark.parametrize('state', ['stale', 'future', 'failed', 'new_cover'])
def test_stale_directory_or_new_uninspected_filing_holds_old_period(db, state):
    collect(db)
    row = db.scalar(select(DirectFeedDocument).where(DirectFeedDocument.feed == discovery.INDEX_FEED))
    if state == 'stale': row.checked_at = NOW - timedelta(hours=49)
    if state == 'future': row.checked_at = NOW + timedelta(seconds=1)
    if state == 'failed': row.status = 'failed'
    if state == 'new_cover':
        raw = index([('0001104659-26-099999', '6-K', '2026-10-14', 'new.htm')])
        store.record_document(db, row, raw, raw.decode(), {'candidates': policy.parse_index(raw, company(), NOW)})
    db.commit()
    assert issuer.prepared_transcript(db, 'MGA', observed_by=NOW)['status'] == 'unavailable'


def test_old_pending_covers_do_not_block_current_and_queries_are_bounded(db):
    client = Client(db)
    extras = [(f'0001104659-25-{i:06d}', '6-K', '2025-07-31', 'old.htm') for i in range(30)]
    client.pages[policy.INDEX_URL] = index(extras)
    first = discovery.collect_discovery(db, client, [company()], limit=2, recheck_hours=24)
    assert first['processed'] == 2 and not first['errors']
    # With a one-request budget, the identified current transcript precedes old covers.
    second = discovery.collect_discovery(db, client, [company()], limit=1, recheck_hours=24)
    assert second['processed'] == 1 and not second['errors']
    assert client.calls == [policy.INDEX_URL, COVER, TRANSCRIPT]
    calls = []
    def count(*args): calls.append(args[2])
    sql_event.listen(db.bind, 'before_cursor_execute', count)
    try: result = issuer.prepared_transcript(db, 'MGA', observed_by=NOW)
    finally: sql_event.remove(db.bind, 'before_cursor_execute', count)
    assert result['status'] == 'prepared' and len(calls) <= 6


@pytest.mark.parametrize('mutation', ['parent', 'date', 'url', 'publisher', 'recursive'])
def test_metadata_cannot_grant_unreviewed_source(db, mutation):
    collect(db)
    row = db.scalar(select(DirectFeedDocument).where(DirectFeedDocument.feed == 'issuer_earnings'))
    m = json.loads(row.metadata_json)
    if mutation == 'parent': m['parent_event']['accession_number'] = '0001104659-26-000000'
    if mutation == 'date': m['published_at'] = '2026-08-01'
    if mutation == 'url': m['url'] = 'https://evil.example/transcript'
    if mutation == 'publisher': m['transcript_publisher'] = 'Someone else'
    if mutation == 'recursive': m['parent_event']['parent_event'] = {}
    with pytest.raises(DirectSourceError): discovery.verified_metadata(db, company(), m)


def test_daily_attempt_bound_on_missing_transcript(db):
    client = Client(db); client.pages[TRANSCRIPT] = b'<html>Unavailable</html>'
    discovery.collect_discovery(db, client, [company()], limit=2, recheck_hours=24)
    result = discovery.collect_discovery(db, client, [company()], limit=2, recheck_hours=24)
    assert len(result['errors']) == 1
    assert discovery.collect_discovery(db, client, [company()], limit=2, recheck_hours=1)['attempted'] == 0
    assert issuer.prepared_transcript(db, 'MGA', observed_by=NOW)['status'] == 'unavailable'

@pytest.mark.parametrize('case', ['exact', 'different', 'duplicate'])
def test_cross_provider_period_identity_never_creates_duplicate(db, case):
    from app.services.research_evidence import upsert_source_document
    collect(db)
    sec = Security(symbol='MGA', name='Synthetic Magna', asset_class='stock')
    db.add(sec); db.commit()
    source = issuer.prepared_transcript(db, 'MGA', observed_by=NOW)
    text = source['content'] if case != 'different' else 'Different existing rendering'
    doc, _ = upsert_source_document(db, security_id=sec.id, document_type='earnings_transcript',
        source_provider='fmp', external_id=f'earnings_transcript:{sec.id}:2026:Q2',
        filing_type='Q2-2026', content=text, published_at=datetime(2026, 7, 31, tzinfo=timezone.utc))
    if case == 'duplicate':
        upsert_source_document(db, security_id=sec.id, document_type='earnings_transcript',
            source_provider='other', external_id='duplicate', filing_type='Q2-2026', content=text)
    db.commit()
    before = [(d.id, d.source_provider, d.content_hash, d.published_at) for d in db.scalars(select(ResearchSourceDocument))]
    result = issuer.prepare_research_document(db, security_id=sec.id, publish_since=date(2026, 7, 1))
    db.commit()
    if case == 'exact': assert not result['created'] and result['document'].id == doc.id
    else: assert result['reason'] == 'existing_period_requires_reconciliation'
    assert before == [(d.id, d.source_provider, d.content_hash, d.published_at) for d in db.scalars(select(ResearchSourceDocument))]
    assert db.scalar(select(func.count()).select_from(ResearchEvidenceEvent)) == 0


def test_changed_same_period_link_quarantines_instead_of_duplicate(db):
    client = collect(db)
    row = db.scalar(select(DirectFeedDocument).where(DirectFeedDocument.feed == discovery.EVENT_FEED))
    row.checked_at = None; db.commit()
    client.pages[COVER] = cover(href='replacement.htm')
    result = discovery.collect_discovery(db, client, [company()], limit=2, recheck_hours=24)
    assert len(result['errors']) == 1
    assert issuer.prepared_transcript(db, 'MGA', observed_by=NOW)['status'] == 'unavailable'
    transcripts = list(db.scalars(select(DirectFeedDocument).where(DirectFeedDocument.feed == 'issuer_earnings')))
    assert len(transcripts) == 1 and transcripts[0].source_url == TRANSCRIPT and transcripts[0].status == 'quarantined'


def test_future_quarter_rollover_holds_prior_until_full_new_chain(db, monkeypatch):
    client = collect(db)
    later = datetime(2026, 11, 2, 18, tzinfo=timezone.utc)
    monkeypatch.setattr(discovery, 'utcnow', lambda: later)
    monkeypatch.setattr(store, 'utcnow', lambda: later)
    new_cover = COVER.replace('000110465926089231', '000110465926099999').replace('tm2618646d2', 'new')
    new_transcript = new_cover.replace('_6k.htm', '_ex99-3.htm')
    client.pages[policy.INDEX_URL] = index([('0001104659-26-099999', '6-K', '2026-10-30', 'new_6k.htm')])
    client.pages[new_cover] = cover(label='Q3 2026 Results Webcast Transcript (October 30, 2026)', href='new_ex99-3.htm', day='October 30, 2026')
    client.pages[new_transcript] = transcript().replace(b'tm2618646d2', b'new').replace(b'SECOND QUARTER', b'THIRD QUARTER').replace(b'26JUL31', b'26OCT30')
    result = discovery.collect_discovery(db, client, [company()], limit=2, recheck_hours=24)
    assert not result['errors']
    assert issuer.prepared_transcript(db, 'MGA', observed_by=later)['status'] == 'unavailable'
    result = discovery.collect_discovery(db, client, [company()], limit=1, recheck_hours=24)
    assert not result['errors'] and client.calls[-1] == new_transcript
    source = issuer.prepared_transcript(db, 'MGA', observed_by=later)
    assert source['status'] == 'prepared' and source['quarter'] == 3 and source['published_date'] == '2026-10-30'
    # Both period originals survive; selection alone changes.
    assert db.scalar(select(func.count()).select_from(DirectFeedDocument).where(DirectFeedDocument.feed == 'issuer_earnings')) == 2


@pytest.mark.parametrize('mutation', ['valid', 'date', 'period', 'qa'])
def test_reviewed_legacy_numeric_date_layout(mutation):
    m = policy.parse_event(cover(), policy.parse_index(index(), company(), NOW)[0], NOW)[0]
    raw = transcript().replace(b'MAGNA INTERNATIONAL - SECOND QUARTER 2026 RESULTS WEBCAST &ndash; 26JUL31',
        b'TRANSCRIPT 07 &ndash; 31 - 2026 Magna International Q2 2026 Results')
    raw = raw.replace(b'QUESTION &amp; ANSWER:', b'QUESTIONS:').replace(b"this does conclude today's conference call", b"This concludes today's call")
    if mutation == 'date': raw = raw.replace(b'07 &ndash; 31', b'07 &ndash; 30')
    if mutation == 'period': raw = raw.replace(b'Q2 2026', b'Q1 2026')
    if mutation == 'qa': raw = raw.replace(b'first question comes from', b'prepared speech by')
    if mutation == 'valid': assert policy.parse_candidate(raw, m, NOW)[2]['has_qa']
    else:
        with pytest.raises(DirectSourceError): policy.parse_candidate(raw, m, NOW)


def test_daily_directory_refresh_reuses_immutable_sec_originals(db, monkeypatch):
    client = collect(db)
    before = [(r.id, r.content_hash, r.fetched_at) for r in db.scalars(select(DirectFeedRevision))]
    later = NOW + timedelta(hours=25)
    monkeypatch.setattr(discovery, 'utcnow', lambda: later)
    monkeypatch.setattr(store, 'utcnow', lambda: later)
    result = discovery.collect_discovery(db, client, [company()], limit=2, recheck_hours=24)
    assert result['attempted'] == result['processed'] == 1 and not result['errors']
    assert client.calls == [policy.INDEX_URL, COVER, TRANSCRIPT, policy.INDEX_URL]
    assert before == [(r.id, r.content_hash, r.fetched_at) for r in db.scalars(select(DirectFeedRevision))]
    assert issuer.prepared_transcript(db, 'MGA', observed_by=later)['status'] == 'prepared'
