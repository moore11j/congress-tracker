"""Operator-run date-correction checks in private rollback-only PostgreSQL tables.

No public data is read or written; the reviewed code bundle runs only in this
disposable process, and every table must resolve to pg_temp before tests begin.
"""
import base64
from datetime import date, datetime, timezone
import hashlib
import json
import sys
import types

from sqlalchemy import Date, DateTime, select, text, func
from sqlalchemy.schema import CreateTable
from sqlalchemy.orm import Session

from app.db import engine
from app.models import (Member, Security, Filing, Transaction, Event, TradeOutcome,
    MonitoringAlert, ResearchEvidenceEvent, DataEnrichmentJob, AppSetting)
from app.services import direct_congress_repair as repair
from app.services import feed_source_control as ownership
from app.services.direct_feed_store import DirectFeedDocument, DirectFeedRevision, dumps


def main(bundle):
    assert engine.dialect.name == 'postgresql'
    source = bundle['source']
    assert hashlib.sha256(source.encode()).hexdigest() == bundle['source_sha256']
    module = types.ModuleType('app.services.direct_congress_dates')
    sys.modules[module.__name__] = module
    exec(compile(source, '<reviewed-congress-dates>', 'exec'), module.__dict__)
    # Fixture locks must not contend with the production feed writer.
    ownership.LOCK_KEYS = {**ownership.LOCK_KEYS, 'house_ptr': 992846513}
    models = (Member, Security, Filing, Transaction, Event, TradeOutcome,
        MonitoringAlert, ResearchEvidenceEvent, DataEnrichmentJob, AppSetting,
        DirectFeedDocument, DirectFeedRevision, ownership.FeedSourceControl,
        repair.CongressRepairArchive, repair.CongressRowBinding, repair.CongressRepairReceipt)
    assert all(m.__table__.schema is None for m in models)
    raw = base64.b64decode(bundle['raw_base64'], validate=True)
    document = dict(raw=raw, content_hash=bundle['case']['sha256'], feed='house_ptr',
        metadata=bundle['case']['metadata'], document_id=1)
    assert hashlib.sha256(raw).hexdigest() == document['content_hash']
    checks=[]
    for scenario in ('success','stale_hash','unpaused','wrong_generation','stale_source','archive_failure'):
        oids=[]
        with engine.connect() as conn:
            conn.detach()
            outer=conn.begin()
            try:
                conn.execute(text('SET LOCAL search_path = pg_temp'))
                conn.execute(text("SET LOCAL statement_timeout = '15s'"))
                conn.execute(text("SET LOCAL lock_timeout = '100ms'"))
                for m in models:
                    ddl=str(CreateTable(m.__table__).compile(dialect=conn.dialect)).strip()
                    assert ddl.startswith('CREATE TABLE ')
                    conn.exec_driver_sql(ddl.replace('CREATE TABLE ','CREATE TEMP TABLE ',1)+' ON COMMIT DROP')
                for _,ddl in repair.postgres_lookup_index_statements(conn.dialect,concurrent=False):
                    conn.exec_driver_sql(ddl)
                assert conn.scalar(text('SHOW search_path'))=='pg_temp'
                for m in models:
                    row=conn.execute(text('SELECT oid, relpersistence, relnamespace=pg_my_temp_schema() AS private '
                        'FROM pg_class WHERE oid=to_regclass(:name)'),{'name':m.__tablename__}).one()
                    assert row.relpersistence=='t' and row.private
                    oids.append(row.oid)
                def session():
                    return Session(bind=conn,autoflush=False,join_transaction_mode='create_savepoint')
                with session() as db:
                    for name,m in [('members',Member),('securities',Security),('filings',Filing),('transactions',Transaction),('events',Event)]:
                        for row in bundle['case']['state'][name]:
                            values=dict(row)
                            for column in m.__table__.columns:
                                if isinstance(values.get(column.name),str):
                                    if isinstance(column.type,DateTime): values[column.name]=datetime.fromisoformat(values[column.name])
                                    elif isinstance(column.type,Date): values[column.name]=date.fromisoformat(values[column.name])
                            db.add(m(**values))
                        db.flush()
                    db.add(DirectFeedDocument(id=1,feed='house_ptr',source_key=document['metadata']['key'],
                        source_url=document['metadata']['url'],metadata_json=dumps(document['metadata']),
                        content_hash=document['content_hash'],status='parsed'))
                    db.add(ownership.FeedSourceControl(feed='house_ptr',provider='paused',generation=1,
                        publish_since=date(2026,10,1),reason='isolated date test',updated_at=datetime.now(timezone.utc)))
                    db.commit()
                    preview=module.inspect_date_correction(db,document,bundle['directory'])
                    assert preview['status']=='planned',preview
                    original={e.id:(e.ts,e.created_at) for e in db.scalars(select(Event))}
                    if scenario=='unpaused':db.get(ownership.FeedSourceControl,'house_ptr').provider='fmp'
                    if scenario=='stale_source':db.get(DirectFeedDocument,1).content_hash='changed'
                    db.commit()
                with session() as db:
                    if scenario=='archive_failure':
                        original_flush=db.flush
                        def fail(*args,**kwargs):
                            if any(isinstance(r,repair.CongressRowBinding) for r in db.new):
                                raise RuntimeError('injected archive failure')
                            return original_flush(*args,**kwargs)
                        db.flush=fail
                    args=dict(expected_before_hash='stale' if scenario=='stale_hash' else preview['before_hash'],
                        expected_generation=2 if scenario=='wrong_generation' else 1)
                    expected={'stale_hash':'population changed','unpaused':'paused source generation',
                        'wrong_generation':'paused source generation','stale_source':'staged source changed',
                        'archive_failure':'injected archive failure'}
                    try:
                        result=module.apply_reviewed_date_correction(db,document,bundle['directory'],**args)
                    except (ValueError,RuntimeError) as exc:
                        assert scenario in expected and expected[scenario] in str(exc),str(exc)
                    else:
                        assert scenario=='success' and result['status']=='applied',result
                        assert module.apply_reviewed_date_correction(db,document,bundle['directory'],**args)['status']=='existing'
                with session() as db:
                    assert db.scalar(select(func.count()).select_from(Event))==len(original)
                    for event in db.scalars(select(Event)):
                        assert (event.ts,event.created_at)==original[event.id]
                        payload=json.loads(event.payload_json)
                        if scenario=='success':
                            assert payload['source_availability']['date']==original[event.id][0].date().isoformat()
                            assert event.event_date.date().isoformat()==document['metadata']['filing_date']
                        else:
                            assert 'source_availability' not in payload
                    assert db.scalar(select(func.count()).select_from(repair.CongressRepairArchive))==(9 if scenario=='success' else 0)
                checks.append(scenario)
            finally:
                outer.rollback()
        with engine.connect() as check:
            assert check.scalar(text('SELECT count(*) FROM pg_class WHERE oid=ANY(:oids)'),{'oids':oids})==0
    print('POSTGRES_DATE_TEST='+dumps(dict(checks=checks,source_sha256=bundle['source_sha256'],
        temporary_tables_only=True,all_test_tables_removed=True,public_data_reads=0,public_data_writes=0,emails=0)),flush=True)
