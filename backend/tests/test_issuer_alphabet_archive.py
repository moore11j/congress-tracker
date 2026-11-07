from copy import deepcopy
from datetime import datetime,timezone
from pathlib import Path
import gzip,hashlib,json,socket
import pytest
from sqlalchemy import create_engine,select,event
from sqlalchemy.orm import Session

from app.db import Base
from app.clients.direct_sources import DirectSourceError
from app.services import issuer_alphabet_discovery as policy,issuer_transcripts as issuer
from app.services import issuer_discovery as discovery,direct_feed_store as store
from app.services.direct_feed_store import DirectFeedDocument,DirectFeedRevision

ROOT=Path(__file__).parent/'fixtures/issuer_alphabet_archive_nov7'
NOW=datetime(2026,11,7,tzinfo=timezone.utc)

def original(name):
    raw=gzip.decompress((ROOT/(name+'.gz')).read_bytes())
    proof=json.loads((ROOT/'manifest.json').read_bytes())[name]
    assert len(raw)==proof['bytes'] and hashlib.sha256(raw).hexdigest()==proof['sha256']
    return raw

def company():
    return deepcopy(next(c for c in json.loads(issuer.REGISTRY.read_bytes()) if c['symbol']=='GOOGL'))

@pytest.fixture(autouse=True)
def offline(monkeypatch):
    def deny(*a,**kw):raise AssertionError('Unexpected external request')
    monkeypatch.setattr(socket.socket,'connect',deny)

def test_complete_archive_and_all_four_originals_repeat_with_independent_fiscal_dates():
    raw=original('reports-2025.json')
    rows=policy.parse_index(raw,company(),2025,NOW)
    assert [r['fiscal_quarter'] for r in rows]==[4,3,2,1]
    names=['archive-2025-0.html','archive-2025-1.html','archive-2025-q2-canonical.html','archive-2025-3.html']
    manifest=json.loads((ROOT/'manifest.json').read_bytes())
    for row,name in zip(rows,names):
        assert row['url']==manifest[name]['url']
        first=policy.parse_candidate(original(name),row,NOW)
        assert first==policy.parse_candidate(original(name),row,NOW)
        resolved,text,parsed=first
        assert parsed['has_qa'] and len(text)>10000
        assert resolved['fiscal_year']==2025 and resolved['source_page_sha256']==hashlib.sha256(raw).hexdigest()
        assert resolved['published_at'].startswith('2026-' if row['fiscal_quarter']==4 else '2025-')
    assert rows==policy.parse_index(raw,company(),2025,NOW)

@pytest.mark.parametrize('mutation',['host','credentials','query','fragment','fiscal_year','quarter','event_year','slug_year','traversal','encoding'])
def test_archive_routes_reject_unreviewed_identity_and_delivery(mutation):
    data=json.loads(original('reports-2025.json'))
    item=next(d for r in data['GetFinancialReportListResult'] for d in r['Documents'] if d['DocumentTitle']=='Webcast & Transcript')
    url=item['DocumentPath']
    replacements={'host':('abc.xyz','example.com'),'credentials':('https://','https://user@'),
        'fiscal_year':('/2025-Q4','/2024-Q4'),'quarter':('Q4','Q3'),'event_year':('/2026/','/2027/'),
        'slug_year':('Call-2026','Call-2025'),'traversal':('/event-details/','/event-details/../'),
        'encoding':('Earnings','%45arnings')}
    if mutation in replacements:url=url.replace(*replacements[mutation])
    elif mutation=='query':url+='?redirect=https://example.com'
    else:url+='#different'
    item['DocumentPath']=url
    with pytest.raises(DirectSourceError):policy.parse_index(json.dumps(data).encode(),company(),2025,NOW)

def test_calendar_rollover_only_permitted_for_fourth_quarter():
    with pytest.raises(DirectSourceError):
        policy.transcript_url('https://abc.xyz/investor/events/event-details/2026/2025-Q1-Earnings-Call/default.aspx',2025,1)
    with pytest.raises(DirectSourceError):
        policy.transcript_url('//example.com/investor/events/event-details/2025/2025-Q1-Earnings-Call/',2025,1)

def test_wrong_original_cannot_satisfy_archive_fiscal_identity():
    rows=policy.parse_index(original('reports-2025.json'),company(),2025,NOW)
    with pytest.raises(DirectSourceError):policy.parse_candidate(original('archive-2025-1.html'),rows[0],NOW)


def test_archive_timezone_does_not_accept_unreviewed_zone():
    row=next(r for r in policy.parse_index(original('reports-2025.json'),company(),2025,NOW) if r['fiscal_quarter']==2)
    raw=original('archive-2025-q2-canonical.html')
    assert b'America/Los Angeles' in raw
    with pytest.raises(DirectSourceError):
        policy.parse_candidate(raw.replace(b'America/Los Angeles',b'Europe/Paris'),row,NOW)


def test_both_years_collect_with_two_request_budget_and_no_repeat_growth(monkeypatch):
    monkeypatch.setattr(discovery,'utcnow',lambda:NOW)
    monkeypatch.setattr(store,'utcnow',lambda:NOW)
    pages={}
    for folder in (ROOT,ROOT.parent/'issuer_alphabet_nov6'):
        for name,proof in json.loads((folder/'manifest.json').read_bytes()).items():
            raw=gzip.decompress((folder/(name+'.gz')).read_bytes())
            assert len(raw)==proof['bytes'] and hashlib.sha256(raw).hexdigest()==proof['sha256']
            pages[proof['url']]=raw
    def clock(mapper,connection,target):target.fetched_at=NOW
    event.listen(DirectFeedRevision,'before_insert',clock)
    engine=create_engine('sqlite:///:memory:');Base.metadata.create_all(engine)
    try:
        with Session(engine) as db:
            class Client:
                issuer_document_urls=frozenset()
                def __init__(self):self.calls=[]
                def get(self,url):
                    assert not db.in_transaction() and url in pages
                    if '/event-details/' in url:assert url in self.issuer_document_urls
                    self.calls.append(url);return pages[url]
            client=Client()
            runs=[discovery.collect_discovery(db,client,[company()],limit=2,recheck_hours=24) for _ in range(4)]
            assert all(r['attempted']==r['processed']==2 and not r['errors'] for r in runs)
            assert len(client.calls)==len(set(client.calls))==8
            def snapshot():
                return store.dumps([[table.name,[dict(r._mapping) for r in db.execute(select(table).order_by(table.c.id))]]
                                    for table in (DirectFeedDocument.__table__,DirectFeedRevision.__table__)])
            before=snapshot();db.commit()
            repeat=discovery.collect_discovery(db,client,[company()],limit=2,recheck_hours=24)
            assert repeat['attempted']==0 and snapshot()==before and len(client.calls)==8
            latest=issuer.prepared_transcript(db,'GOOGL',observed_by=NOW)
            assert latest['status']=='prepared' and (latest['year'],latest['quarter'])==(2026,3)
            assert latest['has_qa']
    finally:
        event.remove(DirectFeedRevision,'before_insert',clock);engine.dispose()
