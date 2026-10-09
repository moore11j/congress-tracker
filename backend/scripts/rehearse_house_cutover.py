"""Offline full-member House publication rehearsal from a read-only export."""
import argparse
import base64
from collections import Counter
from datetime import date, datetime
import hashlib
import json
import os
from pathlib import Path
from unittest.mock import patch


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--capture-dir',required=True,type=Path)
    folder=parser.parse_args().capture_dir.resolve()
    assert folder.is_relative_to((Path(__file__).resolve().parents[2]/'artifacts/direct-feeds').resolve())
    os.environ['DATABASE_URL']='sqlite:///:memory:'
    from sqlalchemy import create_engine, select, Date, DateTime, func
    from sqlalchemy.orm import Session
    from app.db import Base
    from app.models import Member, Security, Filing, Transaction, Event, TradeOutcome, EmailDelivery
    from app.services.direct_feed_store import DirectFeedDocument, DirectFeedRevision, dumps
    from app.services.direct_congress_dates import inspect_date_correction,rehearse_date_correction
    from app.services.direct_congress_worker import publish_document
    from app.services.direct_congress_repair import _digest
    from app.services.direct_feed_collection import parse_document
    from app.services.feed_source_control import select_feed_source
    source=json.loads((folder/'production-baseline.json').read_bytes())
    assert source['transaction_read_only'] and source['provider']=='fmp' and source['generation']==0
    def forbidden(*args,**kwargs):raise AssertionError('Offline House rehearsal attempted network or mail')
    def load(db,model,values):
        values=dict(values)
        for column in model.__table__.columns:
            if isinstance(values.get(column.name),str):
                if isinstance(column.type,DateTime):values[column.name]=datetime.fromisoformat(values[column.name])
                elif isinstance(column.type,Date):values[column.name]=date.fromisoformat(values[column.name])
        db.add(model(**values))
    with patch('socket.socket.connect',forbidden),patch('app.services.email_digests._send_digest',forbidden):
        engine=create_engine('sqlite:///:memory:');Base.metadata.create_all(engine)
        with Session(engine,autoflush=False) as db:
            for key,model in [('members',Member),('securities',Security),('filings',Filing),('transactions',Transaction),('events',Event),('outcomes',TradeOutcome)]:
                for row in source[key]:load(db,model,row)
                db.flush()
            documents=[]
            for entry in source['documents']:
                row=entry['document'];load(db,DirectFeedDocument,row)
                if entry['raw_base64']:
                    raw=base64.b64decode(entry['raw_base64'],validate=True)
                    assert hashlib.sha256(raw).hexdigest()==row['content_hash']
                    db.add(DirectFeedRevision(document_id=row['id'],content_hash=row['content_hash'],source_bytes=raw,source_text='',parsed_json=row['parsed_json'] or '{}'))
                    documents.append(dict(document_id=row['id'],feed='house_ptr',content_hash=row['content_hash'],metadata=json.loads(row['metadata_json']),raw=raw))
            db.commit()
            downstream=None
            if (folder/'downstream.json').exists():
                import house_cutover_alerts as alerts
                downstream=json.loads((folder/'downstream.json').read_bytes())
                user,watch=alerts.prepare(db,downstream,load)
                previews_before=alerts.preview(db,downstream,user,watch)
            before_ids=set(db.scalars(select(Event.id)))
            original_dates={e.id:(e.ts,e.created_at) for e in db.scalars(select(Event))}
            corrected=[];diagnostics=[]
            for document in documents:
                plan=inspect_date_correction(db,document,source['directory'])
                if plan['status']=='planned':
                    result=rehearse_date_correction(db,document,source['directory'],expected_before_hash=plan['before_hash'])
                    assert result['status']=='rehearsed';db.commit()
                    staged=db.get(DirectFeedDocument,document['document_id']);staged.status='parsed';staged.error=None;db.commit()
                    corrected.append(dict(document_id=document['document_id'],**result))
                diagnostics.append(dict(document_id=document['document_id'],date_plan=plan))
            select_feed_source(db,feed='house_ptr',provider='official_house',publish_since=date(2026,10,1),expected_generation=0,reason='Isolated full-member House cutover rehearsal');db.commit()
            results=[]
            for document in documents:
                results.append(dict(document_id=document['document_id'],**publish_document(db,document['document_id'],directory=source['directory'])))
            alert_report=None
            if downstream:
                previews_after=alerts.preview(db,downstream,user,watch)
                alert_report=alerts.verify(db,downstream,user,watch,previews_before,previews_after)
            def fingerprint():return _digest([[table.name,sorted([dict(r._mapping) for r in db.execute(select(table))],key=dumps)] for table in Base.metadata.sorted_tables])
            before=fingerprint()
            for result in results:
                if result['status'] in {'published','existing'}:
                    assert publish_document(db,result['document_id'],directory=source['directory'])['inserted_events']==0
            assert before==fingerprint()
            for identifier,times in original_dates.items():
                event=db.get(Event,identifier);assert (event.ts,event.created_at)==times
            assert db.scalar(select(func.count()).select_from(EmailDelivery))==0
            report=dict(captured_at=source['captured_at'],counts={k:len(source[k]) for k in ['documents','members','securities','filings','transactions','events']},
                date_corrections=corrected,results=results,diagnostics=diagnostics,
                source_without_bytes=[e['document']['id'] for e in source['documents'] if not e['raw_base64']],
                statuses=dict(Counter(r['status'] for r in results)),new_events=len(set(db.scalars(select(Event.id)))-before_ids),
                original_ids_and_times_preserved=True,repeat_unchanged=True,emails=0,production_writes=0,alert_report=alert_report)
            (folder/'cutover-rehearsal.json').write_text(dumps(report),encoding='utf-8')
            print(dumps({k:v for k,v in report.items() if k not in {'diagnostics'}}))


if __name__=='__main__':main()
