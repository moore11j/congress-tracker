"""Replay the captured October 7 institutional cohort and prior-quarter sources."""
import argparse
import base64
from collections import Counter
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import os
import time
from pathlib import Path
from unittest.mock import patch


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--baseline',type=Path,required=True)
    parser.add_argument('--priors',type=Path,required=True)
    parser.add_argument('--mapping-parents',type=Path,help='Supplemental parents for an older baseline without mapping_filings')
    parser.add_argument('--identifiers',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--prepared-evidence',action='store_true')
    args=parser.parse_args()
    root=Path(__file__).resolve().parents[2]/'artifacts/direct-feeds'
    assert all(p.resolve().is_relative_to(root.resolve()) for p in (args.baseline,args.priors,args.identifiers,args.output,args.mapping_parents) if p is not None)
    os.environ['DATABASE_URL']='sqlite:///:memory:'
    from sqlalchemy import Date,DateTime,select,func
    from app.db import Base,engine,SessionLocal
    from app.models import (InstitutionalFiling,InstitutionalPosition,InstitutionalHolder,InstitutionalSymbolSummary,
                            InstitutionalPositionChange,InstitutionalActivityEvent,Event,EmailDelivery,
                            UserAccount,Watchlist,WatchlistItem,Security,MonitoringAlert)
    from app.services.direct_feed_store import discover,record_document,dumps
    from app.services.direct_feed_collection import parse_document
    from app.services.direct_13f_batch import load_identifier_manifest
    from app.services.direct_13f_worker import publish_13f_document
    from app.services.direct_13f_evidence import Prepared13FEvidence
    from app.services.feed_source_control import select_feed_source
    from app.services.monitoring_alerts import _ensure_alert_for_event
    from app.services import email_digests as digests
    assert engine.dialect.name=='sqlite' and engine.url.database==':memory:'
    baseline_raw=args.baseline.read_bytes();baseline_hash=hashlib.sha256(baseline_raw).hexdigest()
    baseline=json.loads(baseline_raw);del baseline_raw
    assert baseline['transaction_read_only'] and baseline['filing_date']=='2026-10-07'
    parents=(json.loads(args.mapping_parents.read_bytes()) if args.mapping_parents else
             dict(filings=baseline['mapping_filings'],transaction_read_only=True,baseline_sha256=baseline_hash))
    assert parents['transaction_read_only'] and parents['baseline_sha256']==baseline_hash
    manifest=json.loads(args.priors.read_bytes())
    priors=[]
    for source in manifest['results']:
        if source['status']!='parsed':continue
        path=(args.priors.parent/source['source_file']).resolve()
        history=(args.priors.parent/source['history_file']).resolve()
        assert path.parent==history.parent==args.priors.parent.resolve()
        raw=path.read_bytes();assert hashlib.sha256(raw).hexdigest()==source['content_hash']
        assert hashlib.sha256(history.read_bytes()).hexdigest()==source['history_sha256']
        priors.append({**source,'raw':raw})
    assert 1<=len(priors)<=20
    selected_ids={r['current_document_id'] for r in priors}
    current=[];comparisons=[]
    for source in baseline['documents']:
        if not source['raw_base64']:continue
        raw=base64.b64decode(source['raw_base64'])
        assert hashlib.sha256(raw).hexdigest()==source['content_hash']
        document={**source,'feed':'sec_13f','raw':raw}
        if source['status']=='parsed':comparisons.append(document)
        if source['id'] in selected_ids:
            assert source['status']=='parsed'
            current.append(document)
    assert len(current)==len(priors)
    identifiers=load_identifier_manifest(args.identifiers)
    documents=sorted([*priors,*current],key=lambda d:(d['metadata']['filing_date'],d['metadata']['key']))
    comparisons.extend(priors)
    started=time.monotonic()
    prepared=Prepared13FEvidence.build(identifiers,comparisons) if args.prepared_evidence else None
    Base.metadata.create_all(engine)
    def forbidden(*a,**k):raise AssertionError('Offline replay attempted transport or email')
    with patch('requests.sessions.Session.request',forbidden),patch('httpx.Client.send',forbidden), \
         patch('socket.socket.connect',forbidden),patch.object(digests,'_send_digest',forbidden),SessionLocal() as db:
        def load(model,rows):
            for row in rows:
                record={}
                for column in model.__table__.columns:
                    value=row[column.name]
                    if value is not None:
                        if isinstance(column.type,DateTime):value=datetime.fromisoformat(value)
                        elif isinstance(column.type,Date):value=date.fromisoformat(value)
                    record[column.name]=value
                db.add(model(**record))
            db.flush()
        parent_rows={r['id']:r for r in parents['filings']}
        for row in baseline['filings']:
            if row['id'] in parent_rows:assert row==parent_rows[row['id']]
            parent_rows[row['id']]=row
        assert {r['filing_id'] for r in baseline['mapping_positions']} <= set(parent_rows)
        load(InstitutionalFiling,parent_rows.values())
        positions={r['id']:r for r in [*baseline['mapping_positions'],*baseline['positions']]}
        load(InstitutionalPosition,positions.values())
        load(InstitutionalHolder,baseline['holders'])
        for model,key in [(InstitutionalPositionChange,'changes'),(InstitutionalActivityEvent,'activity'),(Event,'events')]:
            load(model,baseline[key])
        # Current Q3 projections can update Q3 summaries only. Historical Q2
        # imports precede the publication boundary and cannot create changes.
        summaries=[r for r in baseline['summaries'] if r['report_year']==2026 and r['report_quarter']==3]
        assert not summaries, 'Review newly present Q3 derived baseline before replay'
        db.commit()
        staged_ids={}
        for source in documents:
            text,parsed,holds=parse_document('sec_13f',source['raw'],source['metadata'])
            assert not holds
            assert parsed['metadata']['report_period'] in {'2026-06-30','2026-09-30'}
            staged=discover(db,'sec_13f',source['metadata'])
            record_document(db,staged,source['raw'],text,parsed)
            staged_ids[source['metadata']['key']]=staged.id
        db.commit()
        select_feed_source(db,feed='sec_13f',provider='sec_edgar',publish_since=date(2026,10,7),
                           expected_generation=0,reason='Isolated current source and prior-quarter cutover replay')
        db.commit()
        def run(verbose=False):
            results=[]
            for source in documents:
                result=publish_13f_document(db,staged_ids[source['metadata']['key']],
                    identifier_documents=identifiers,comparison_documents=comparisons,prepared_evidence=prepared)
                results.append(dict(accession=source['metadata']['key'],cik=source['metadata']['cik'],**result))
                if verbose:print(dumps(dict(accession=source['metadata']['key'],status=result['status'],
                    derived_state=result.get('derived_state'),feed_events=result.get('feed_events',0))),flush=True)
            return results
        first=run(True)
        events=[e for e in db.scalars(select(Event)) if json.loads(e.payload_json or '{}').get('sec_verification',{}).get('feed')=='sec_13f']
        previews=None
        if events:
            user=UserAccount(email='institutional-cutover@example.test',entitlement_tier='pro',watchlist_activity_notifications=True)
            db.add(user);db.flush()
            watchlist=Watchlist(name='Isolated institutional cutover',owner_user_id=user.id)
            db.add(watchlist);db.flush()
            for symbol in sorted({e.symbol for e in events}):
                security=Security(symbol=symbol,name=symbol,asset_class='stock');db.add(security);db.flush()
                db.add(WatchlistItem(watchlist_id=watchlist.id,security_id=security.id))
            db.commit()
            for event in events:assert _ensure_alert_for_event(db,user_id=user.id,watchlist=watchlist,event=event)
            db.commit()
            since=min(e.ts for e in events).replace(tzinfo=timezone.utc)-timedelta(seconds=1)
            end=max(e.ts for e in events).replace(tzinfo=timezone.utc)+timedelta(seconds=1)
            class FrozenDatetime(datetime):
                @classmethod
                def now(cls,tz=None):return end.astimezone(tz) if tz else end.replace(tzinfo=None)
            def preview():
                with patch.object(digests,'datetime',FrozenDatetime),patch.object(digests,'_upcoming_calendar_events_for_digest',return_value=([],'Calendar outside institutional replay')):
                    builds=[digests.build_monitoring_digest(db,user,watchlist,since,window_end=end),
                            digests.build_signal_alert_digest(db,user,since,window_end=end),
                            digests.build_watchlist_activity_digest(db,user,watchlist,since)]
                assert all(b.items_count>0 for b in builds)
                return [dict(items=b.items_count,template=b.template_key,context=b.context) for b in builds]
            previews=preview()
        def fingerprint():
            digest=hashlib.sha256()
            for table in Base.metadata.sorted_tables:
                rows=sorted([dict(r._mapping) for r in db.execute(select(table))],key=dumps)
                digest.update(dumps([table.name,rows]).encode())
            return digest.hexdigest()
        before=fingerprint();repeat=run()
        assert fingerprint()==before
        assert all(not r.get('feed_events') and not r.get('inserted_positions') and not r.get('inserted_filings') for r in repeat)
        if events:
            for event in events:assert not _ensure_alert_for_event(db,user_id=user.id,watchlist=watchlist,event=event)
            assert preview()==previews and fingerprint()==before
        assert db.scalar(select(func.count()).select_from(EmailDelivery))==0
        report=dict(baseline_sha256=baseline_hash,baseline_captured_at=baseline['captured_at'],source_documents=len(documents),
            prepared_evidence=args.prepared_evidence,elapsed_seconds=round(time.monotonic()-started,2),
            statuses=dict(Counter(r['status'] for r in first)),derived_states=dict(Counter(r.get('derived_state',r['status']) for r in first)),
            results=first,totals={key:sum(r.get(key,0) for r in first) for key in ['inserted_filings','inserted_positions','changes','summaries','activity_events','feed_events']},
            qualifying_events=[dict(symbol=e.symbol,event_type=e.event_type,payload=json.loads(e.payload_json)) for e in events],
            no_send_previews=previews,repeat_state_identical=True,state_sha256=before,production_writes=0,email_deliveries=0)
        args.output.write_text(dumps(report),encoding='utf-8')
        print(dumps({k:v for k,v in report.items() if k not in ['results','qualifying_events','no_send_previews']}),flush=True)


if __name__=='__main__':main()
