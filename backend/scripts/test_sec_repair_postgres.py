"""Exercise the exact SEC repair in private PostgreSQL tables, then roll back.

The supplied bundle is reviewed local code and synthetic records. All ORM table
names must resolve to pg_temp; no production records are read or modified.
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


def main(bundle):
    from app.db import engine
    from app.models import Event, InsiderTransaction, InsiderTransactionNormalized, MonitoringAlert, DataEnrichmentJob, AppSetting
    from app.services import feed_source_control as ownership
    from app.services.direct_feed_store import DirectFeedDocument, DirectFeedRevision, dumps
    from app.services.direct_feed_worker import DirectFeedPublication
    assert engine.dialect.name == 'postgresql'
    for name, item in bundle['modules'].items():
        assert hashlib.sha256(item['source'].encode()).hexdigest() == item['sha256']
        module = types.ModuleType(name)
        sys.modules[name] = module
        exec(compile(item['source'], '<reviewed-sec-repair>', 'exec'), module.__dict__)
    repair = sys.modules['app.services.direct_sec_repair']
    ownership.LOCK_KEYS = {**ownership.LOCK_KEYS, 'sec_form4': 992846519}
    models = (Event, InsiderTransaction, InsiderTransactionNormalized, MonitoringAlert, DataEnrichmentJob,
        AppSetting, DirectFeedDocument, DirectFeedRevision, DirectFeedPublication,
        ownership.FeedSourceControl, repair.SecRepairReceipt)
    assert all(model.__table__.schema is None for model in models)
    checks = []
    scenarios = bundle.get('scenarios', ('success', 'stale_hash', 'wrong_generation', 'unpaused', 'source_change', 'receipt_failure'))
    assert set(scenarios) <= {'success', 'stale_hash', 'wrong_generation', 'unpaused', 'source_change', 'receipt_failure'}
    for scenario in scenarios:
        private_oids = []
        with engine.connect() as conn:
            conn.detach()
            outer = conn.begin()
            try:
                conn.execute(text('SET LOCAL search_path = pg_temp'))
                conn.execute(text("SET LOCAL statement_timeout = '20s'"))
                for model in models:
                    ddl = str(CreateTable(model.__table__).compile(dialect=conn.dialect)).strip()
                    assert ddl.startswith('CREATE TABLE ')
                    conn.exec_driver_sql(ddl.replace('CREATE TABLE ', 'CREATE TEMP TABLE ', 1) + ' ON COMMIT DROP')
                for model in models:
                    private = conn.execute(text('SELECT oid, relpersistence, relnamespace=pg_my_temp_schema() AS private '
                        'FROM pg_class WHERE oid=to_regclass(:name)'), {'name': model.__tablename__}).one()
                    assert private.relpersistence == 't' and private.private
                    private_oids.append(private.oid)
                def session():
                    return Session(bind=conn, autoflush=False, join_transaction_mode='create_savepoint')
                with session() as db:
                    for model in models:
                        for item in bundle['records'].get(model.__tablename__, []):
                            row = dict(item)
                            for column in model.__table__.columns:
                                value = row.get(column.name)
                                if isinstance(value, str):
                                    if isinstance(column.type, DateTime): row[column.name] = datetime.fromisoformat(value)
                                    elif isinstance(column.type, Date): row[column.name] = date.fromisoformat(value)
                            if model is DirectFeedRevision:
                                row['source_bytes'] = base64.b64decode(row['source_bytes'])
                            db.add(model(**row))
                        db.flush()
                    db.add(ownership.FeedSourceControl(feed='sec_form4', provider='paused', generation=1,
                        publish_since=date(2026, 6, 1), reason='private repair test', updated_at=datetime.now(timezone.utc)))
                    db.commit()
                    review = repair.inspect_staged_repair(db, 1)
                    assert review['status'] == 'planned', review
                    original = {e.id: (e.ts, e.event_date) for e in db.scalars(select(Event))}
                    if scenario == 'unpaused': db.get(ownership.FeedSourceControl, 'sec_form4').provider = 'fmp'
                    if scenario == 'source_change': db.get(DirectFeedRevision, 1).source_bytes += b'changed'
                    db.commit()
                with session() as db:
                    if scenario == 'receipt_failure':
                        original_flush = db.flush
                        def fail(*args, **kwargs):
                            if any(isinstance(row, repair.SecRepairReceipt) for row in db.new):
                                raise RuntimeError('injected audit failure')
                            return original_flush(*args, **kwargs)
                        db.flush = fail
                    expected = {'stale_hash': 'population changed', 'wrong_generation': 'paused source generation',
                        'unpaused': 'paused source generation', 'source_change': 'source bytes',
                        'receipt_failure': 'injected audit failure'}
                    try:
                        result = repair.apply_reviewed_staged_repair(db, 1,
                            expected_plan_hash='stale' if scenario == 'stale_hash' else review['plan_sha256'],
                            expected_generation=2 if scenario == 'wrong_generation' else 1)
                        assert scenario == 'success' and result['updated'] == 6
                        assert repair.apply_reviewed_staged_repair(db, 1,
                            expected_plan_hash=review['plan_sha256'], expected_generation=1)['updated'] == 0
                    except (ValueError, RuntimeError) as error:
                        assert scenario in expected and expected[scenario] in str(error), (scenario, str(error))
                with session() as db:
                    assert {e.id: (e.ts, e.event_date) for e in db.scalars(select(Event))} == original
                    assert db.scalar(select(func.count()).select_from(Event)) == 3
                    assert db.scalar(select(func.count()).select_from(repair.SecRepairReceipt)) == (1 if scenario == 'success' else 0)
                    if scenario != 'success':
                        assert all(row.price == 999 for row in db.scalars(select(InsiderTransactionNormalized)))
                checks.append(scenario)
            finally:
                outer.rollback()
        with engine.connect() as check:
            assert check.scalar(text('SELECT count(*) FROM pg_class WHERE oid=ANY(:oids)'), {'oids': private_oids}) == 0
        print(json.dumps(dict(completed=scenario, private_tables_removed=True)), flush=True)
    print(json.dumps(dict(status='passed', checks=checks, public_reads=0, public_writes=0, emails=0)))
