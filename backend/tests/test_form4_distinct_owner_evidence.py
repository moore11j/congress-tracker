from datetime import date
import hashlib,json
import pytest
from sqlalchemy import select,func
from app.models import Event,InsiderTransactionNormalized,SecForm4Filing,EmailDelivery
from app.services import direct_feed_worker as worker
from app.services.direct_feed_collection import parse_document
from app.services.direct_feed_store import DirectFeedDocument,DirectFeedRevision,discover,record_document
from test_direct_feed_worker import db,activate,counts
from test_direct_feed_rehearsal import form4_document


def stage_owner(db,owner='0001111111',suffix='000001'):
    doc=form4_document(repeat=True)
    key='0000320193-26-'+suffix
    raw=doc['raw'].replace(b'0001111111',owner.encode()).replace(b'0000320193-26-000001',key.encode())
    metadata={**doc['metadata'],'key':key,'url':f'https://www.sec.gov/Archives/edgar/data/320193/{key}.txt'}
    source,parsed,reasons=parse_document('sec_form4',raw,metadata)
    assert not reasons
    row=discover(db,'sec_form4',metadata);record_document(db,row,raw,source,parsed)
    db.commit()
    return row.id


def test_distinct_verified_owners_same_ticker_dates_and_economics_publish_without_duplicates(db):
    activate(db)
    first=stage_owner(db)
    assert worker.publish_document(db,first)['inserted_events']==4
    original_events=[(e.id,e.payload_json,e.ts) for e in db.scalars(select(Event).order_by(Event.id))]
    second=stage_owner(db,'0002222222','000002')
    result=worker.publish_document(db,second)
    assert result['status']=='published' and result['inserted_events']==4
    assert counts(db)[:3]==[2,8,8]
    before=counts(db)
    assert worker.publish_document(db,first)['inserted_events']==0
    assert worker.publish_document(db,second)['inserted_events']==0
    assert counts(db)==before
    assert original_events==[(e.id,e.payload_json,e.ts) for e in db.scalars(select(Event).order_by(Event.id).limit(4))]
    assert db.scalar(select(func.count()).select_from(EmailDelivery))==0


def test_same_owner_in_different_accession_remains_held(db):
    activate(db)
    first=stage_owner(db);worker.publish_document(db,first)
    second=stage_owner(db,suffix='000002')
    assert worker.publish_document(db,second)['status']=='held'
    assert counts(db)[:3]==[1,4,4]


@pytest.mark.parametrize('fault',['provider','event_id','event_hash','event_payload','event_owner',
    'event_url','receipt_missing','receipt_hash','receipt_event_ids','receipt_metadata',
    'original_missing','original_tampered','staged_quarantine','normalized_price','normalized_owner','filing_hash'])
def test_different_owner_without_complete_unchanged_sec_evidence_stays_held(db,fault):
    activate(db)
    first=stage_owner(db);worker.publish_document(db,first)
    event=db.scalar(select(Event).order_by(Event.id))
    payload=json.loads(event.payload_json)
    receipt=db.scalar(select(worker.DirectFeedPublication))
    revision=db.scalar(select(DirectFeedRevision))
    normalized=db.get(InsiderTransactionNormalized,payload['normalized_transaction_id'])
    if fault=='provider':event.source_provider='fmp'
    elif fault=='event_id':payload['normalized_transaction_id']=999999
    elif fault=='event_hash':event.source_filing_id='wrong'
    elif fault=='event_payload':payload['shares']=999
    elif fault=='event_owner':payload['reporting_cik']='0009999999'
    elif fault=='event_url':event.source_document_url='https://example.com/not-sec'
    elif fault=='receipt_missing':db.delete(receipt)
    elif fault=='receipt_hash':receipt.source_hash='wrong'
    elif fault=='receipt_event_ids':
        report=json.loads(receipt.report_json);report['event_ids']=[];receipt.report_json=json.dumps(report)
    elif fault=='receipt_metadata':receipt.metadata_hash='wrong'
    elif fault=='original_missing':revision.source_bytes=None
    elif fault=='original_tampered':revision.source_bytes+=b'changed'
    elif fault=='staged_quarantine':db.get(DirectFeedDocument,first).status='quarantined'
    elif fault=='normalized_price':normalized.price=999
    elif fault=='normalized_owner':normalized.reporting_owner_cik='0009999999'
    elif fault=='filing_hash':db.scalar(select(SecForm4Filing)).document_hash='wrong'
    event.payload_json=json.dumps(payload);db.commit()
    second=stage_owner(db,'0002222222','000002')
    assert worker.publish_document(db,second)['status']=='held'
    assert counts(db)[:3]==[1,4,4]


def test_original_parsed_once_per_atomic_projection_for_repeated_lots(db,monkeypatch):
    from app.services import sec_form4_candidate_evidence as evidence
    activate(db);first=stage_owner(db);worker.publish_document(db,first)
    second=stage_owner(db,'0002222222','000002')
    real=evidence.parse_document;calls=[]
    def tracked(*args,**kwargs):
        calls.append(1);return real(*args,**kwargs)
    monkeypatch.setattr(evidence,'parse_document',tracked)
    assert worker.publish_document(db,second)['inserted_events']==4
    assert len(calls)==1


@pytest.mark.parametrize('budget',[0,1,8*1024*1024])
def test_cache_budget_never_truncates_source_evidence_or_blocks_valid_owner(db,monkeypatch,budget):
    from app.services.sec_form4_candidate_evidence import OtherOwnerEvidence
    monkeypatch.setattr(OtherOwnerEvidence,'MAX_BYTES',budget)
    activate(db);first=stage_owner(db);worker.publish_document(db,first)
    second=stage_owner(db,'0002222222','000002')
    assert worker.publish_document(db,second)['inserted_events']==4


def test_source_proof_is_read_only_and_cache_does_not_survive_projection(db):
    from sqlalchemy import text
    from app.services.sec_form4_candidate_evidence import OtherOwnerEvidence
    activate(db);first=stage_owner(db);worker.publish_document(db,first)
    second=stage_owner(db,'0002222222','000002')
    current=json.loads(db.get(DirectFeedDocument,second).parsed_json)['transactions'][0]
    event=db.scalar(select(Event).order_by(Event.id));payload=json.loads(event.payload_json)
    db.execute(text('PRAGMA query_only=ON'))
    try:
        evidence=OtherOwnerEvidence(db)
        assert evidence.is_distinct(event,payload,current)
        assert len(evidence.cache)==1 and evidence.bytes<=evidence.MAX_BYTES
        assert not db.new and not db.dirty and not db.deleted
    finally:db.execute(text('PRAGMA query_only=OFF'))
    db.rollback()
    revision=db.scalar(select(DirectFeedRevision).where(DirectFeedRevision.document_id==first))
    revision.source_bytes+=b'tampered-after-proof';db.commit()
    assert worker.publish_document(db,second)['status']=='held'
    assert counts(db)[:3]==[1,4,4]


def test_invalid_original_with_updated_transport_hash_is_still_held(db):
    activate(db);first=stage_owner(db);worker.publish_document(db,first)
    revision=db.scalar(select(DirectFeedRevision));old_hash=revision.content_hash
    revision.source_bytes=revision.source_bytes.replace(b'<documentType>4</documentType>',b'<documentType>4/A</documentType>')
    new_hash=hashlib.sha256(revision.source_bytes).hexdigest();assert new_hash!=old_hash
    revision.content_hash=new_hash
    db.get(DirectFeedDocument,first).content_hash=new_hash
    db.scalar(select(worker.DirectFeedPublication)).source_hash=new_hash
    for event in db.scalars(select(Event)):
        payload=json.loads(event.payload_json);payload['sec_verification']['sha256']=new_hash;event.payload_json=json.dumps(payload)
    db.commit()
    second=stage_owner(db,'0002222222','000002')
    assert worker.publish_document(db,second)['status']=='held'
    assert counts(db)[:3]==[1,4,4]


def test_original_cache_eviction_keeps_every_verified_source_eligible(db,monkeypatch):
    from app.services.sec_form4_candidate_evidence import OtherOwnerEvidence
    monkeypatch.setattr(OtherOwnerEvidence,'MAX_SOURCES',1)
    activate(db)
    for index in range(1,4):
        document=stage_owner(db,str(index)*7,str(index).zfill(6))
        assert worker.publish_document(db,document)['inserted_events']==4
    assert counts(db)[:3]==[3,12,12]
    assert worker.publish_batch(db,retry_held=True)['processed']==0
