"""Exact SEC publication/research code against disposable loopback PostgreSQL.

No production URL is accepted. A fresh private schema is removed on completion.
"""
import argparse
from datetime import date
import hashlib
import json
from pathlib import Path
import uuid
from unittest.mock import patch

from sqlalchemy import create_engine, func, select, text
from sqlalchemy.orm import Session

from app.db import Base
from app.models import Event, ResearchSourceDocument, Security
from app.services.direct_feed_store import DirectFeedDocument, DirectFeedRevision
from app.services.direct_feed_worker import DirectFeedPublication
from app.services.feed_source_control import FeedSourceControl, FeedWriterBusy, FeedSourceMismatch, select_feed_source
from app.services.sec_earnings_store import stage_earnings_material
from app.services.sec_press_events import FEED, sync_sec_release_events
from app.services.sec_press_research import prepare_sec_research_document
from app.services.operational_intelligence import _ingest_article


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', type=int, required=True)
    parser.add_argument('--sources', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if not 1024 <= args.port <= 65535:
        raise ValueError('Expected a local PostgreSQL port')
    row = next(r for r in json.loads(args.sources.read_text())['sources'] if r['symbol'] == 'LEVI')
    bundle = {}
    for key, suffix, digest in [('company_raw','-company.json','company_sha256'),
                               ('submission_raw','.txt','source_sha256'),('index_raw','-index.html','index_sha256')]:
        raw = (args.sources.parent / ('LEVI'+suffix)).read_bytes()
        if hashlib.sha256(raw).hexdigest() != row[digest]:
            raise ValueError('Captured source hash mismatch')
        bundle[key] = raw
    url = f'postgresql+psycopg://postgres@127.0.0.1:{args.port}/walnut_sec_replay'
    schema = 'sec_press_test_' + uuid.uuid4().hex
    control = create_engine(url, connect_args={'connect_timeout':5})
    engine = None
    created = False
    report = {'database_scope':'disposable_loopback', 'checks':[], 'network_source_requests':0, 'emails_sent':0}
    models = (Security, Event, ResearchSourceDocument, DirectFeedDocument, DirectFeedRevision,
              DirectFeedPublication, FeedSourceControl)
    try:
        with control.begin() as conn:
            assert conn.scalar(text('SELECT current_database()')) == 'walnut_sec_replay'
            report['postgresql_version'] = conn.scalar(text('SHOW server_version'))
            conn.exec_driver_sql(f'CREATE SCHEMA {schema}')
            created = True
        engine = create_engine(url, connect_args={'connect_timeout':5,
            'options':f'-c search_path={schema} -c lock_timeout=2000 -c statement_timeout=10000'})
        Base.metadata.create_all(engine, tables=[m.__table__ for m in models])
        with engine.connect() as conn:
            assert conn.scalar(text('SELECT current_schema()')) == schema
            for model in models:
                assert conn.scalar(text('SELECT n.nspname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace '
                                        'WHERE c.oid=to_regclass(:name)'), {'name':model.__tablename__}) == schema
        with Session(engine) as db:
            db.add(Security(id=1, symbol='LEVI', name='Levi Strauss', asset_class='stock')); db.commit()
            staged = stage_earnings_material(db, **bundle, symbol='LEVI', cik=row['cik'],
                accession=row['filing']['accession_number']); db.commit()
            select_feed_source(db, feed=FEED, provider='sec_edgar',
                publish_since=date.fromisoformat(row['filing']['filing_date']), expected_generation=0, reason='isolated PostgreSQL validation')
            db.commit()
        def prepare(db):
            return prepare_sec_research_document(db, security_id=1, accession=row['filing']['accession_number'])
        with Session(engine) as writer, Session(engine) as contender:
            first = prepare(writer)
            assert first['status'] == 'prepared' and first['created']
            try:
                prepare(contender)
            except FeedWriterBusy:
                contender.rollback()
            else:
                raise AssertionError('Concurrent writer entered a locked publication')
            report['checks'].append('concurrent_publisher_refused')
            try:
                select_feed_source(contender, feed=FEED, provider='paused',
                    publish_since=date.fromisoformat(row['filing']['filing_date']), expected_generation=1, reason='isolated pause')
            except FeedWriterBusy:
                contender.rollback()
            else:
                raise AssertionError('Source switch entered an active publication')
            report['checks'].append('source_switch_refused_while_writing')
            writer.rollback()
            for model in (Event, ResearchSourceDocument, DirectFeedPublication):
                assert contender.scalar(select(func.count()).select_from(model)) == 0
            contender.rollback()
            report['checks'].append('event_research_receipt_rollback_atomic')
            committed = prepare(contender); contender.commit()
            assert committed['created']
            saved_id = committed['document'].id
            repeated = prepare(writer); writer.commit()
            assert not repeated['created'] and repeated['document'].id == saved_id
            report['checks'].append('retry_and_repeat_preserve_identity')
        with Session(engine) as db:
            assert sync_sec_release_events(db, ['LEVI']) == 0
            db.commit()
            report['checks'].append('postgres_jsonb_candidate_query_repeat_zero')
            try:
                _ingest_article(db, security=db.get(Security,1), document_type='press_release',
                    item={'title':'Legacy cached text must not create a second source', 'summary':'Retained FMP cache'})
            except FeedSourceMismatch:
                db.rollback()
            else:
                raise AssertionError('Retired press writer entered the direct source')
            report['checks'].append('retired_research_writer_refused')
            report['counts'] = {m.__tablename__: db.scalar(select(func.count()).select_from(m)) for m in models}
            assert report['counts']['events'] == report['counts']['research_source_documents'] == report['counts']['direct_feed_publications'] == 1
            assert report['counts']['direct_feed_documents'] == report['counts']['direct_feed_revisions'] == 3
    finally:
        if engine is not None:
            engine.dispose()
        if created:
            with control.begin() as conn:
                conn.exec_driver_sql(f'DROP SCHEMA {schema} CASCADE')
                assert conn.scalar(text('SELECT count(*) FROM pg_namespace WHERE nspname=:schema'), {'schema':schema}) == 0
            report['schema_removed'] = True
        control.dispose()
    args.output.write_text(json.dumps(report, sort_keys=True, indent=2)+'\n')
    print(json.dumps(report, sort_keys=True))


if __name__ == '__main__':
    with patch('requests.sessions.Session.request', side_effect=AssertionError('Unexpected source/model request')):
        main()
