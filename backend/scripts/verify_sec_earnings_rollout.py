"""Read-only scheduled SEC earnings preparation receipt; public data only."""
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import platform

files = ['app/jobs/warm_sec_earnings.py', 'app/services/sec_company_filings.py',
         'app/services/sec_earnings_materials.py', 'app/services/sec_earnings_store.py',
         'app/services/sec_press_releases.py', 'app/services/sec_directory.py',
         'app/jobs/collect_direct_feeds.py', 'crontab']
report = {'observed_at': datetime.now(timezone.utc).isoformat(), 'runtime': platform.python_version(),
    'hashes': {name: hashlib.sha256(Path('/app', name).read_bytes()).hexdigest() for name in files},
    'flags': {key: os.getenv(key) for key in ('SEC_EARNINGS_WARMING_ENABLED', 'PRESS_RELEASE_PROVIDER',
        'SEC_RESEARCH_WARMING_ENABLED', 'FINANCIAL_STATEMENTS_PROVIDER', 'COMPANY_METADATA_PROVIDER',
        'FUNDAMENTALS_PROVIDER', 'STOCK_PRICE_PROVIDER', 'NEWS_PROVIDER', 'NEWS_PUBLISH_AFTER')}}
if os.getenv('SEC_EARNINGS_HASH_ONLY') != '1':
    from sqlalchemy import func, select, text
    from sqlalchemy.orm import Session
    from app.db import engine
    from app.models import InsightsSnapshot, ResearchSourceDocument, TickerContentCache
    from app.services.direct_feed_store import DirectFeedDocument, DirectFeedRevision
    from app.services.direct_feed_worker import DirectFeedPublication
    assert engine.dialect.name == 'postgresql'
    with engine.connect() as conn, conn.begin():
        conn.execute(text('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY'))
        conn.execute(text("SET LOCAL statement_timeout='20s'"))
        assert conn.scalar(text('SHOW transaction_read_only')) == 'on'
        with Session(bind=conn) as db:
            row = db.get(InsightsSnapshot, 'sec-earnings:warming:v1')
            state = json.loads(row.payload_json) if row else {}
            report['runs'] = state.get('runs', [])[-20:]
            report['lease_until'] = (state.get('lease') or {}).get('until')
            report['attempted_symbols'] = sorted(state.get('attempted_at', {}))
            rows = list(db.scalars(select(TickerContentCache).where(
                TickerContentCache.content_type == 'sec_earnings_releases',
                TickerContentCache.source == 'sec_edgar_earnings').order_by(TickerContentCache.symbol).limit(101)))
            report['public_caches'] = [{'symbol': row.symbol, 'fetched_at': str(row.fetched_at),
                                       'payload': json.loads(row.payload_json)} for row in rows]
            report['cache_count'] = len(rows)
            report['caches_truncated'] = len(rows) == 101
            report['release_links'] = sum(row.item_count or 0 for row in rows)
            report['source_counts'] = [dict(feed=feed, status=status, count=count) for feed, status, count in
                db.execute(select(DirectFeedDocument.feed, DirectFeedDocument.status, func.count())
                    .where(DirectFeedDocument.feed.like('sec_earnings_%'))
                    .group_by(DirectFeedDocument.feed, DirectFeedDocument.status))]
            report['source_revisions'] = db.scalar(select(func.count()).select_from(DirectFeedRevision)
                .join(DirectFeedDocument, DirectFeedDocument.id == DirectFeedRevision.document_id)
                .where(DirectFeedDocument.feed.like('sec_earnings_%')))
            report['publication_receipts'] = db.scalar(select(func.count()).select_from(DirectFeedPublication)
                .where(DirectFeedPublication.feed == 'sec_earnings_release'))
            report['research_documents'] = db.scalar(select(func.count()).select_from(ResearchSourceDocument)
                .where(ResearchSourceDocument.source_provider == 'sec_edgar_earnings'))
            report.update(transaction_read_only=True, database_writes=0, customer_rows_exported=0)
print('SEC_EARNINGS_RECEIPT=' + json.dumps(report))
