from copy import deepcopy
from datetime import date
import json
import pytest
from issuer_fixture_clock import source_clock
from sqlalchemy import select,func
from app.clients.direct_sources import DirectSourceError,parse_issuer_material
from app.models import Base,Security,ResearchSourceDocument,ResearchEvidenceEvent
from app.services import issuer_moderna_discovery as policy,issuer_discovery as discovery,issuer_transcripts as issuer
from test_issuer_moderna_discovery import NOW,company,index,event,transcript,Client,db,no_network

HEADING='LSEG STREETEVENTS EDITED TRANSCRIPT MRNA.OQ - Q3 2026 Moderna, Inc. Earnings Call EVENT DATE/TIME: OCTOBER 09, 2026 / 12:00PM GMT '

@pytest.mark.parametrize('name',['Moderna, Inc.','Moderna Inc.','Moderna, Inc'])
def test_legal_name_punctuation_preserves_metadata(name):
    metadata=policy.parse_index(index(),company(),NOW)[0]
    original=deepcopy(metadata)
    raw=transcript(HEADING.replace('Moderna, Inc.',name))
    resolved,text,parsed=policy.parse_candidate(raw,metadata,NOW)
    assert resolved==metadata==original
    assert parsed==parse_issuer_material(raw,metadata)[1]
    assert parsed['period_pattern']==original['period_pattern']
    assert parsed['canonical_key']=='MRNA:earnings_transcript:2026:Q3'
    assert parsed['has_qa'] and name in text

@pytest.mark.parametrize('before,after',[
    ('MRNA.OQ','OTHER.OQ'),('Moderna, Inc.','Another Moderna, Inc.'),
    ('Moderna, Inc.','Moderna Holdings, Inc.'),('Q3 2026','Q2 2026'),
    ('Q3 2026','Q3 2025'),('OCTOBER 09','OCTOBER 08'),('LSEG','REFINITIV')])
def test_punctuation_does_not_relax_heading_identity(before,after):
    metadata=policy.parse_index(index(),company(),NOW)[0]
    # A correct title later in the body must not excuse a wrong heading.
    raw=transcript(HEADING.replace(before,after)+HEADING)
    with pytest.raises(DirectSourceError):policy.parse_candidate(raw,metadata,NOW)

def test_punctuated_document_still_requires_qa():
    metadata=policy.parse_index(index(),company(),NOW)[0]
    with pytest.raises(DirectSourceError):policy.parse_candidate(transcript(HEADING,qa=False),metadata,NOW)

@pytest.mark.parametrize('field,value',[('symbol','OTHER'),('fiscal_quarter',True),('fiscal_year','2026'),('period_pattern',r'.*')])
def test_punctuation_permission_requires_exact_reviewed_metadata(field,value):
    metadata=policy.parse_index(index(),company(),NOW)[0]
    metadata[field]=value
    with pytest.raises(DirectSourceError):policy.parse_candidate(transcript(HEADING),metadata,NOW)

def test_other_policies_do_not_inherit_punctuation_permission():
    metadata=policy.parse_index(index(),company(),NOW)[0]
    metadata.pop('discovery_kind')
    with pytest.raises(DirectSourceError):parse_issuer_material(transcript(HEADING),metadata)

def test_existing_unpunctuated_result_is_exactly_unchanged():
    metadata=policy.parse_index(index(),company(),NOW)[0]
    raw=transcript();text,parsed=parse_issuer_material(raw,metadata)
    assert policy.parse_candidate(raw,metadata,NOW)==(metadata,text,parsed)

def test_collector_reader_document_and_full_repeat(db):
    class Punctuated(Client):
        def get(self,url):
            assert not self.db.in_transaction();self.calls.append(url)
            if url==policy.INDEX_URL:return index()
            assert url==event()['transcriptURL'] and url in self.issuer_document_urls
            return transcript(HEADING)
    client=Punctuated(db)
    result=discovery.collect_discovery(db,client,[company()],limit=2,recheck_hours=24)
    assert result['processed']==2 and not result['errors']
    assert issuer.prepared_transcript(db,'MRNA')['quarter']==3
    security=Security(symbol='MRNA',name='Synthetic Moderna',asset_class='stock');db.add(security);db.commit()
    assert issuer.prepare_research_document(db,security_id=security.id,publish_since=date(2026,10,1))['created'];db.commit()
    before={t.name:[dict(r._mapping) for r in db.execute(select(t))] for t in Base.metadata.sorted_tables}
    assert not issuer.prepare_research_document(db,security_id=security.id,publish_since=date(2026,10,1))['created'];db.commit()
    assert discovery.collect_discovery(db,client,[company()],limit=2,recheck_hours=24)['attempted']==0
    assert before=={t.name:[dict(r._mapping) for r in db.execute(select(t))] for t in Base.metadata.sorted_tables}
    assert len(client.calls)==2
    assert db.scalar(select(func.count()).select_from(ResearchSourceDocument))==1
    assert db.scalar(select(func.count()).select_from(ResearchEvidenceEvent))==0
