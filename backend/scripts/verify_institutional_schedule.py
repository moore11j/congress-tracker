"""Read-only institutional staging schedule receipt; no customer or source bodies."""
from collections import Counter
from datetime import date, datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import platform

FILES = ['app/services/direct_13f_priors.py', 'app/jobs/collect_direct_13f_priors.py',
         'app/services/direct_13f_evidence.py', 'app/services/direct_13f_batch.py',
         'app/services/direct_13f_identifiers.py', 'app/services/direct_13f_publication.py',
         'app/services/direct_13f_worker.py', 'app/jobs/publish_direct_13fs.py', 'crontab']
report = {'observed_at': datetime.now(timezone.utc).isoformat(), 'runtime': platform.python_version(),
          'hashes': {name: hashlib.sha256(Path('/app', name).read_bytes()).hexdigest() for name in FILES},
          'flags': {key: os.getenv(key) for key in ('DIRECT_FEEDS_MODE', 'DIRECT_13F_PUBLICATION_ENABLED')}}
if os.getenv('INSTITUTIONAL_HASH_ONLY') != '1':
    from sqlalchemy import select, text, func
    from sqlalchemy.orm import Session
    from app.db import engine
    from app.models import InstitutionalFiling
    from app.services.direct_feed_store import DirectFeedDocument, DirectFeedRevision, DirectFeedRun
    from app.services.direct_feed_worker import DirectFeedPublication
    from app.services.feed_source_control import FeedSourceControl
    assert engine.dialect.name == 'postgresql'
    with engine.connect() as conn, conn.begin():
        conn.execute(text('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY'))
        conn.execute(text("SET LOCAL statement_timeout='20s'"))
        assert conn.scalar(text('SHOW transaction_read_only')) == 'on'
        with Session(bind=conn) as db:
            control = db.get(FeedSourceControl, 'sec_13f')
            report['institutional_provider'] = control.provider if control else 'fmp'
            report['publication_receipts'] = db.scalar(select(func.count()).select_from(DirectFeedPublication)
                .join(DirectFeedDocument, DirectFeedDocument.id == DirectFeedPublication.document_id)
                .where(DirectFeedDocument.feed == 'sec_13f'))
            report['q3_canonical_filings'] = db.scalar(select(func.count()).select_from(InstitutionalFiling)
                .where(InstitutionalFiling.report_period_end == date(2026, 9, 30)))
            runs = list(db.scalars(select(DirectFeedRun).where(DirectFeedRun.report_json.contains('sec_13f_priors'))
                .order_by(DirectFeedRun.id.desc()).limit(12)))
            report['runs'] = []
            for run in runs:
                data = json.loads(run.report_json)
                results = data.get('results', [])
                report['runs'].append({'id': run.id, 'started_at': str(run.started_at),
                    'finished_at': str(run.finished_at), 'status': run.status,
                    'requested': data.get('requested'), 'processed': data.get('processed'),
                    'results': results, 'public_writes': data.get('public_writes'),
                    'counts': dict(Counter(row['status'] for row in results))})
            rows = list(db.execute(select(DirectFeedDocument.id, DirectFeedDocument.content_hash,
                DirectFeedDocument.metadata_json, DirectFeedDocument.checked_at,
                DirectFeedDocument.reconciliation_json).where(DirectFeedDocument.feed == 'sec_13f')
                .order_by(DirectFeedDocument.id).limit(20001)))
            assert len(rows) <= 20000
            report['source_fingerprints'] = [{'id': row.id, 'hash': row.content_hash,
                'metadata_hash': hashlib.sha256(row.metadata_json.encode()).hexdigest(),
                'checked_at': str(row.checked_at),
                'prior_attempt': json.loads(row.reconciliation_json or '{}').get('_prior_collection')}
                for row in rows]
            report['revision_counts'] = dict(db.execute(select(DirectFeedRevision.document_id, func.count())
                .where(DirectFeedRevision.document_id.in_([row.id for row in rows]))
                .group_by(DirectFeedRevision.document_id)).all())
            report.update(transaction_read_only=True, database_writes=0, customer_rows_exported=0)
print('INSTITUTIONAL_RECEIPT=' + json.dumps(report))
