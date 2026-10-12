import base64
from copy import deepcopy
from datetime import date, timedelta
import json

import pytest
from sqlalchemy import select, func, event as sql_event

from app.models import Event, Filing, Transaction, EmailDelivery, Security
from app.services import senate_backup_publication as publisher
from app.services.senate_backup_collection import stage_bundle, FEED
from app.services.senate_backup_evidence import PROVIDER
from app.services.direct_feed_store import DirectFeedDocument, DirectFeedRevision
from app.services.direct_congress_repair import CongressRowBinding
from app.services.feed_source_control import select_feed_source, FeedSourceMismatch, FeedSourceControl
from test_senate_backup_collection import bundle
from test_senate_backup_evidence import NOW, SNAPSHOT, case
from test_senate_backup_reconciliation import directory
from test_direct_congress_worker import db, stage, counts
from app.services import direct_congress_worker as official


def staged(db, bundle):
    stage_bundle(db, bundle, now=NOW); db.commit()
    identifier = db.scalar(select(DirectFeedDocument.id).where(
        DirectFeedDocument.feed==FEED, DirectFeedDocument.source_key==case()[0]['docId']))
    db.rollback()
    return identifier


def switch(db, provider=PROVIDER, generation=0, boundary=date(2026,9,1)):
    select_feed_source(db, feed='senate_ptr', provider=provider, publish_since=boundary,
        expected_generation=generation, reason='isolated secondary recovery test')
    db.commit()


def publish(db, identifier, **kwargs):
    return publisher.publish_document(db, identifier, directory=directory(), now=NOW, **kwargs)


def originals(db):
    raw = base64.b64decode(SNAPSHOT['originals_base64'][case()[0]['docId']])
    return stage(db, raw=raw)[0]


def public_state(db):
    from app.services.direct_congress_repair import filing_state
    return deepcopy(filing_state(db, case()[0]['sourceUrl']))


def test_new_secondary_publication_preserves_provenance_and_repeats(db,bundle):
    identifier=staged(db,bundle);switch(db)
    result=publish(db,identifier)
    assert result['status']=='published' and result['inserted_events']==result['inserted_transactions']==5
    assert counts(db)[:4]==[1,5,5,0]
    before=public_state(db);bindings=result['bindings']
    for event in db.scalars(select(Event)):
        payload=json.loads(event.payload_json)
        assert event.source_provider==PROVIDER
        assert payload['source_line_ref'] is None and payload['secondary_row_id']==event.source_filing_id
        assert payload['source_verification']['original_verified'] is False
        assert payload['source_verification']['evidence_kind']=='secondary_extraction'
        assert payload['partial_sale'] is True
        assert payload['source_availability']['date']==NOW.date().isoformat()
        assert payload['filing_date']=='2026-10-01' and payload['trade_date']=='2026-09-04'
        from app.services.backtesting.queries import event_entry_date
        assert event_entry_date(event,payload)==NOW.date()
    repeated=publish(db,identifier)
    assert repeated['status']=='existing' and repeated['inserted_events']==repeated['inserted_transactions']==0
    assert repeated['bindings']==bindings and public_state(db)==before
    assert db.scalar(select(func.count()).select_from(EmailDelivery))==0


def test_official_recovery_and_return_to_backup_preserve_all_events(db,bundle):
    identifier=staged(db,bundle);switch(db);publish(db,identifier)
    before=public_state(db)
    original=originals(db);switch(db,'official_senate',1)
    result=official.publish_document(db,original,directory=directory())
    assert result['status']=='existing' and result['inserted_events']==result['inserted_transactions']==0
    assert db.scalar(select(func.count()).select_from(CongressRowBinding))==5
    assert public_state(db)==before
    assert official.publish_document(db,original,directory=directory())['status']=='existing'
    switch(db,PROVIDER,2)
    assert publish(db,identifier)['status']=='existing' and public_state(db)==before


def test_original_first_then_backup_adopts_without_rewriting_source(db,bundle):
    original=originals(db);switch(db,'official_senate')
    official.publish_document(db,original,directory=directory())
    before=public_state(db)
    identifier=staged(db,bundle);switch(db,PROVIDER,1)
    result=publish(db,identifier)
    assert result['status']=='existing' and result['inserted_events']==0
    assert public_state(db)==before
    assert all(e.source_provider=='official_senate' for e in db.scalars(select(Event)))


@pytest.mark.parametrize('provider',['fmp','official_senate','paused'])
def test_inactive_backup_cannot_publish(db,bundle,provider):
    identifier=staged(db,bundle)
    if provider!='fmp':switch(db,provider)
    with pytest.raises(FeedSourceMismatch):publish(db,identifier)
    assert counts(db)==[0]*6


def test_missing_boundary_stale_generation_and_boundary_changes_refused(db,bundle):
    identifier=staged(db,bundle);switch(db)
    with pytest.raises(ValueError,match='generation'):switch(db,'official_senate',0)
    db.rollback()
    with pytest.raises(ValueError,match='immutable'):switch(db,'official_senate',1,date(2026,10,1))
    db.rollback()
    control=db.get(FeedSourceControl,'senate_ptr');control.publish_since=None;db.commit()
    with pytest.raises(FeedSourceMismatch,match='boundary'):publish(db,identifier)
    assert counts(db)==[0]*6


@pytest.mark.parametrize('fault',['queue','cache','receipt'])
def test_atomic_secondary_failure_rolls_back_entire_publication(db,bundle,monkeypatch,fault):
    identifier=staged(db,bundle);switch(db)
    def fail(*args,**kwargs):raise RuntimeError('injected interruption')
    if fault=='queue':monkeypatch.setattr(publisher,'enqueue_feed_pnl_enrichment_for_event',fail)
    elif fault=='cache':monkeypatch.setattr(publisher,'bump_feed_events_epoch',fail)
    else:
        def before_flush(session,*args):
            if any(isinstance(row,publisher.DirectFeedPublication) for row in session.new):fail()
        sql_event.listen(db,'before_flush',before_flush)
    with pytest.raises(RuntimeError,match='injected'):publish(db,identifier)
    assert counts(db)==[0]*6


@pytest.mark.parametrize('damage',['raw','parsed','stale','security','canonical'])
def test_tampered_or_incompatible_state_never_adds_more_rows(db,bundle,damage):
    identifier=staged(db,bundle);switch(db)
    if damage=='canonical':
        publish(db,identifier)
        event=db.scalar(select(Event));event.amount_min=1;db.commit()
    elif damage=='security':
        db.add(Security(symbol=case()[1][0]['printedTicker'],name='wrong type',asset_class='option'));db.commit()
    elif damage=='raw':
        revision=db.scalar(select(DirectFeedRevision).where(DirectFeedRevision.document_id==identifier))
        revision.source_bytes+=b'!';db.commit()
    elif damage=='parsed':
        db.get(DirectFeedDocument,identifier).parsed_json='{}';db.commit()
    before=counts(db)
    if damage in {'raw','parsed','stale'}:
        with pytest.raises(ValueError):
            publisher.publish_document(db,identifier,directory=directory(),now=NOW+timedelta(days=2) if damage=='stale' else NOW)
    else:assert publish(db,identifier)['status']=='held'
    assert counts(db)[:5]==before[:5]


def test_completed_days_boundary_and_same_day_refused(db,bundle):
    identifier=staged(db,bundle);switch(db,boundary=date(2026,10,2))
    assert publish(db,identifier)['status']=='held' and counts(db)==[0]*6


def test_recovery_rejects_changed_original_even_with_same_economics(db,bundle):
    identifier=staged(db,bundle);switch(db);publish(db,identifier)
    # Reduced HTML retains economics but is not the original claimed by backup.
    original,_=stage(db);switch(db,'official_senate',1)
    before=public_state(db)
    result=official.publish_document(db,original,directory=directory())
    assert result['status']=='held' and 'disagrees' in result['reason']
    assert public_state(db)==before
    assert db.scalar(select(func.count()).select_from(CongressRowBinding))==0


def test_backup_blocks_unscoped_legacy_writer(db,bundle):
    from app.ingest_senate import upsert_senate_transaction_from_row
    from app.backfill_events_from_trades import insert_missing_congress_events_from_transactions
    staged(db,bundle);switch(db)
    with pytest.raises(FeedSourceMismatch):upsert_senate_transaction_from_row(db,{})
    db.rollback()
    assert insert_missing_congress_events_from_transactions(db)==0


def test_non_stock_rows_remain_transactions_without_equity_alerts(db,bundle):
    staged(db,bundle);switch(db)
    identifier=db.scalar(select(DirectFeedDocument.id).where(
        DirectFeedDocument.feed==FEED,DirectFeedDocument.source_key==case(0)[0]['docId']))
    people=[dict(id={'bioguide':'F000479'},name={'first':'John','last':'Fetterman'},
        terms=[dict(type='sen',state='PA',party='Democrat',start='2023-01-03',end='2029-01-03')])]
    result=publisher.publish_document(db,identifier,directory=people,now=NOW)
    assert result['status']=='published' and result['non_stock_rows']==result['inserted_transactions']==2
    assert result['inserted_events']==0
    assert publisher.publish_document(db,identifier,directory=people,now=NOW)['status']=='existing'
    assert counts(db)[:4]==[1,2,0,0]
    assert db.scalar(select(func.count()).select_from(EmailDelivery))==0


def test_identical_lots_keep_distinct_bound_ids_on_repeat(db,tmp_path,monkeypatch):
    import test_senate_backup_collection as fixtures
    source=deepcopy(SNAPSHOT)
    filing=next(f for f in source['filings'] if f['docId']==case()[0]['docId'])
    filing['extractedRows']+=1
    duplicate=deepcopy(next(row for row in source['trades'] if row['docId']==filing['docId']))
    duplicate['rowIndex']=max(r['rowIndex'] for r in source['trades'] if r['docId']==filing['docId'])+1
    source['trades'].append(duplicate)
    monkeypatch.setattr(fixtures,'SNAPSHOT',source)
    source_bundle=fixtures.bundle.__wrapped__(tmp_path)
    identifier=staged(db,source_bundle);switch(db)
    first=publish(db,identifier)
    assert first['inserted_events']==first['inserted_transactions']==6
    before=public_state(db)
    assert len({b['transaction_id'] for b in first['bindings']})==6
    repeated=publish(db,identifier)
    assert repeated['status']=='existing' and repeated['inserted_events']==repeated['inserted_transactions']==0
    assert repeated['bindings']==first['bindings'] and public_state(db)==before


def test_actual_no_send_consumers_keep_ids_after_recovery(db,bundle,monkeypatch):
    from datetime import datetime, timezone
    from app.models import UserAccount, Watchlist, WatchlistItem, NotificationSubscription
    from app.services.monitoring_alerts import _ensure_alert_for_event
    from app.services import email_digests as digests
    from app.services.email_intraday import _watchlist_candidate
    identifier=staged(db,bundle);switch(db);publish(db,identifier)
    user=UserAccount(email='senate-backup@example.test',entitlement_tier='pro',watchlist_activity_notifications=True)
    db.add(user);db.flush()
    watchlist=Watchlist(name='Isolated Senate evidence',owner_user_id=user.id)
    db.add(watchlist);db.flush()
    db.add(NotificationSubscription(email=user.email,source_type='watchlist',source_id=str(watchlist.id),
        source_name=watchlist.name,frequency='daily',only_if_new=True,active=True,
        source_payload_json='{}',alert_triggers_json=json.dumps(['congress_activity'])))
    for security in db.scalars(select(Security)):
        db.add(WatchlistItem(watchlist_id=watchlist.id,security_id=security.id))
    db.commit()
    events=list(db.scalars(select(Event)))
    for event in events:assert _ensure_alert_for_event(db,user_id=user.id,watchlist=watchlist,event=event)
    db.commit()
    end=NOW+timedelta(seconds=1)
    class Frozen(datetime):
        @classmethod
        def now(cls,tz=None):return end.astimezone(tz) if tz else end.replace(tzinfo=None)
    monkeypatch.setattr(digests,'datetime',Frozen)
    monkeypatch.setattr(digests,'_upcoming_calendar_events_for_digest',lambda *a,**k:([],'Calendar outside Senate test',None))
    def previews():
        start=NOW-timedelta(seconds=1)
        builds=[digests.build_monitoring_digest(db,user,watchlist,start,window_end=end),
            digests.build_signal_alert_digest(db,user,start,window_end=end),
            digests.build_watchlist_activity_digest(db,user,watchlist,start)]
        assert [b.items_count for b in builds]==[5,5,5]
        return [(b.template_key,b.context) for b in builds]
    before=previews()
    keys=[_watchlist_candidate(db,user,watchlist,event).event_key for event in events]
    assert keys==[f'event:{event.id}' for event in events]
    original=originals(db);switch(db,'official_senate',1)
    assert official.publish_document(db,original,directory=directory())['status']=='existing'
    assert previews()==before
    for event in events:assert not _ensure_alert_for_event(db,user_id=user.id,watchlist=watchlist,event=event)
    assert [_watchlist_candidate(db,user,watchlist,event).event_key for event in events]==keys
    assert db.scalar(select(func.count()).select_from(EmailDelivery))==0
