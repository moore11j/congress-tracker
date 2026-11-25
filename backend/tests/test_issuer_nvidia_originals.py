from copy import deepcopy
from datetime import datetime,date
from pathlib import Path
import hashlib,json,os,socket
import pytest
from sqlalchemy import create_engine,select,func
from sqlalchemy.orm import Session
from issuer_fixture_clock import source_clock
from app.db import Base
from app.models import Security,ResearchSourceDocument,ResearchEvidenceEvent,EmailDelivery
from app.services import issuer_nvidia_discovery as policy,issuer_discovery as discovery,issuer_transcripts as issuer,direct_feed_store as store
from app.services.research_evidence import upsert_source_document,EVIDENCE_PROCESSING_VERSION

MANIFEST=json.loads((Path(__file__).parent/'fixtures/issuer_nvidia_nov25/manifest.json').read_bytes())
NOW=datetime.fromisoformat(MANIFEST['observed_at'])

def original(name):
    root=os.getenv('WALNUT_NVIDIA_SOURCE_DIR')
    if not root:pytest.skip('Complete public sources require the explicit fixture directory')
    raw=(Path(root)/name).read_bytes();proof=MANIFEST['files'][name]
    assert len(raw)==proof['bytes'] and hashlib.sha256(raw).hexdigest()==proof['sha256']
    return raw

def company():return deepcopy(next(c for c in json.loads(issuer.REGISTRY.read_bytes()) if c['symbol']=='NVDA'))

def test_existing_static_q2_proof_remains_exact():
    from app.clients.direct_sources import parse_issuer_material
    proof=MANIFEST['static_baseline']
    metadata=issuer._reviewed_latest('NVDA')
    assert metadata==proof['metadata']
    result=parse_issuer_material(original('nvda-2027-q2.pdf'),metadata)
    assert hashlib.sha256(json.dumps(result,sort_keys=True).encode()).hexdigest()==proof['complete_output_sha256']

@pytest.fixture(autouse=True)
def offline(monkeypatch):
    def denied(*a,**kw):raise AssertionError('Unexpected external request')
    monkeypatch.setattr(socket.socket,'connect',denied)
    monkeypatch.setattr(discovery,'utcnow',lambda:NOW);monkeypatch.setattr(store,'utcnow',lambda:NOW)

@pytest.mark.parametrize('name',[n for n in MANIFEST['files'] if n.endswith('.pdf')])
def test_complete_original_exact_output_and_repeat(name):
    proof=MANIFEST['files'][name]['expected']
    metadata=next(m for m in policy.parse_index(original('index-'+proof['key'].split(':')[2]+'.json'),company(),int(proof['key'].split(':')[2]),NOW) if m['key']==proof['key'])
    first=policy.parse_candidate(original(name),metadata,NOW)
    assert first==policy.parse_candidate(original(name),metadata,NOW)
    assert first==policy.parse_candidate(original(name),first[0],NOW)
    assert first[0]['published_at']==proof['published_at'] and first[2]['has_qa']
    assert {k:v for k,v in first[0].items() if k not in ('published_at','publication_pattern')}=={k:v for k,v in metadata.items() if k!='publication_pattern'}
    assert hashlib.sha256(json.dumps(first,sort_keys=True).encode()).hexdigest()==proof['complete_output_sha256']

@pytest.mark.parametrize('historical',[False,True])
def test_complete_collector_and_research_history_repeat(historical):
    pages={p['url']:original(n) for n,p in MANIFEST['files'].items()}
    engine=create_engine('sqlite:///:memory:');Base.metadata.create_all(engine)
    def snapshot(db):return store.dumps([[t.name,sorted([dict(r._mapping) for r in db.execute(select(t))],key=store.dumps)] for t in Base.metadata.sorted_tables])
    try:
        with Session(engine) as db:
            class Client:
                issuer_document_urls=frozenset()
                def __init__(self):self.calls=[]
                def get(self,url):
                    assert not db.in_transaction() and url in pages
                    if url not in {policy.index_url(company(),y) for y in (2026,2027)}:assert url in self.issuer_document_urls
                    self.calls.append(url);return pages[url]
            client=Client()
            runs=[discovery.collect_discovery(db,client,[company()],limit=2,recheck_hours=24) for _ in range(4)]
            assert [r['attempted'] for r in runs]==[2,2,2,2]
            assert all(r['attempted']==r['processed'] and not r['errors'] for r in runs)
            # The configured Q2 static proof belongs to the existing static
            # collector and must not be reacquired by discovery.
            static_key=issuer._reviewed_latest('NVDA')['key']
            static_url=next(p['url'] for p in MANIFEST['files'].values() if p.get('expected',{}).get('key')==static_key)
            expected_urls=set(pages)-{static_url}
            assert set(client.calls)==expected_urls and len(client.calls)==8
            result=issuer.prepared_transcript(db,'NVDA',observed_by=NOW)
            assert result['status']=='prepared' and (result['year'],result['quarter'])==(2027,3)
            assert result['published_date']=='2026-11-17' and result['has_qa']
            security=Security(symbol='NVDA',name='NVIDIA Corp.',asset_class='stock');db.add(security);db.commit()
            old=None
            if historical:
                old,_=upsert_source_document(db,security_id=security.id,document_type='earnings_transcript',source_provider='fmp',
                    external_id=f'earnings_transcript:{security.id}:2027:Q3',content='Synthetic previous provider rendering.',filing_type='Q3-2027',published_at=datetime(2026,11,17,tzinfo=NOW.tzinfo))
                old.processing_status='processed';old.processing_version=EVIDENCE_PROCESSING_VERSION;db.commit()
                prior={col.name:getattr(old,col.name) for col in old.__table__.columns}
            boundary=date(2026,11,18) if historical else date(2026,11,1)
            first=issuer.prepare_research_document(db,security_id=security.id,publish_since=boundary);db.commit()
            assert first['status']==('retained_history' if historical else 'prepared')
            saved=snapshot(db)
            second=issuer.prepare_research_document(db,security_id=security.id,publish_since=boundary);db.commit()
            assert snapshot(db)==saved and first['document'].id==second['document'].id
            assert discovery.collect_discovery(db,client,[company()],limit=2,recheck_hours=24)['attempted']==0
            assert snapshot(db)==saved and len(client.calls)==8
            assert db.scalar(select(func.count()).select_from(ResearchSourceDocument))==1
            assert db.scalar(select(func.count()).select_from(ResearchEvidenceEvent))==0
            assert db.scalar(select(func.count()).select_from(EmailDelivery))==0
            if old is not None:assert {col.name:getattr(old,col.name) for col in old.__table__.columns}==prior
    finally:engine.dispose()
