"""Fresh-source recovery must preserve published identities and evidence clocks."""
from copy import deepcopy
from datetime import timedelta
import hashlib
import json

import pytest
from sqlalchemy import select,func

from app.models import Event,EmailDelivery
from app.services.direct_feed_store import DirectFeedDocument,DirectFeedRevision
from app.services.senate_backup_collection import stage_bundle,FEED
from app.services.senate_backup_publication import publish_document,publish_batch
from test_senate_backup_collection import bundle
from test_senate_backup_evidence import NOW
from test_senate_backup_reconciliation import directory
from test_senate_backup_publication import staged,switch,public_state
from test_direct_congress_worker import db


def revisions(db):
    return [(row.document_id,row.content_hash,row.source_bytes,row.parsed_json)
            for row in db.scalars(select(DirectFeedRevision).order_by(DirectFeedRevision.id))]


def fresh_bundle(bundle,clock):
    fresh=deepcopy(bundle)
    snapshot=json.loads(fresh['manifest'])
    snapshot['generatedAt']=snapshot['lastSourceCheckAt']=clock.isoformat()
    fresh['manifest']=json.dumps(snapshot).encode()
    fresh['observed_at']=clock.isoformat()
    return fresh


def test_stale_collection_and_publication_recover_without_rewriting_existing_rows(db,bundle):
    identifier=staged(db,bundle);switch(db)
    first=publish_document(db,identifier,directory=directory(),now=NOW)
    assert first['inserted_events']==5
    before=public_state(db);saved=revisions(db)
    doc=db.get(DirectFeedDocument,identifier)
    evidence=(doc.content_hash,doc.parsed_json);db.rollback()
    stale=NOW+timedelta(hours=40)
    for attempt in (lambda:stage_bundle(db,bundle,now=stale),
                    lambda:publish_batch(db,directory=directory(),now=stale)):
        with pytest.raises(ValueError,match='freshness'):attempt()
        db.rollback()
        assert public_state(db)==before and revisions(db)==saved
        db.rollback()
    fresh=fresh_bundle(bundle,stale)
    stage_bundle(db,fresh,now=stale);db.commit()
    doc=db.get(DirectFeedDocument,identifier)
    assert (doc.content_hash,doc.parsed_json)==evidence
    assert json.loads(doc.metadata_json)['snapshot_sha256']==hashlib.sha256(fresh['manifest']).hexdigest()
    db.rollback()
    repeated=publish_document(db,identifier,directory=directory(),now=stale)
    assert repeated['status']=='existing' and repeated['inserted_events']==repeated['inserted_transactions']==0
    assert repeated['bindings']==first['bindings'] and public_state(db)==before
    assert db.scalar(select(func.count()).select_from(EmailDelivery))==0


def test_changed_economic_evidence_after_gap_is_held_not_silently_rewritten(db,bundle,tmp_path,monkeypatch):
    import test_senate_backup_collection as fixtures
    from test_senate_backup_evidence import SNAPSHOT,case
    identifier=staged(db,bundle);switch(db)
    publish_document(db,identifier,directory=directory(),now=NOW)
    before=public_state(db)
    source=deepcopy(SNAPSHOT)
    row=next(r for r in source['trades'] if r['docId']==case()[0]['docId'])
    assert row['amountLow']==15001.0 and row['amountHigh']==50000.0
    row['amountBracket']='$50,001 - $100,000'
    row['amountLow']=50001.0
    row['amountHigh']=100000.0
    # Use the actual decoder schema but change a retained economic input.
    monkeypatch.setattr(fixtures,'SNAPSHOT',source)
    changed=fixtures.bundle.__wrapped__(tmp_path)
    clock=NOW+timedelta(hours=40)
    changed=fresh_bundle(changed,clock)
    stage_bundle(db,changed,now=clock);db.commit()
    result=publish_document(db,identifier,directory=directory(),now=clock)
    assert result['status']=='held'
    assert public_state(db)==before
    assert db.scalar(select(func.count()).select_from(Event))==5
    assert db.scalar(select(func.count()).select_from(EmailDelivery))==0
